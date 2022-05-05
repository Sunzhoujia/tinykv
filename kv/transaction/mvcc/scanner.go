package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	StartKey []byte
	Txn      *MvccTxn
	iter     engine_util.DBIterator
	prevKey  []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(startKey)
	return &Scanner{
		StartKey: startKey,
		Txn:      txn,
		iter:     iter,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
	scan.Txn.Reader.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for ; scan.iter.Valid(); scan.iter.Next() {
		item := scan.iter.Item()
		k := item.Key()

		userKey := DecodeUserKey(k)
		if bytes.Equal(userKey, scan.prevKey) {
			continue
		}
		commitTs := decodeTimestamp(k)
		if commitTs <= scan.Txn.StartTS {
			scan.prevKey = userKey
			scan.iter.Next()

			val, err := item.Value()
			if err != nil {
				return userKey, nil, err
			}
			write, err := ParseWrite(val)
			if err != nil {
				return userKey, nil, err
			}
			ts := write.StartTS
			val, err = scan.Txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, ts))
			if err != nil {
				return userKey, nil, err
			}
			return userKey, val, nil
		}
	}

	return nil, nil, nil
}
