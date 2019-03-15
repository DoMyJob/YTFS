package ytfs

import (
	"encoding/binary"
	"encoding/json"
	"path"
	"unsafe"

	ydcommon "github.com/yottachain/YTFS/common"
	"github.com/yottachain/YTFS/opt"
	"github.com/yottachain/YTFS/storage"

	"github.com/syndtr/goleveldb/leveldb"
	lvldbopt "github.com/syndtr/goleveldb/leveldb/opt"
)

var dataCntQueryStr = []byte("dataCnt")

type levelDBHelper struct {
	// data count
	dataCnt uint32
	// key string
	dataCntQueryKey []byte
}

// IndexDB key value db for hash <-> position.
type IndexDB struct {
	// meta data
	schema *ydcommon.Header

	// index file
	indexFile *storage.YTFSIndexFile

	// level db wrapper
	db       *leveldb.DB
	dbHelper levelDBHelper
}

// NewIndexDB creates a new index db based on input file if it's exist.
func NewIndexDB(dir string, config *opt.Options) (*IndexDB, error) {
	fileName := path.Join(dir, "index.db")
	if opt.UseLevelDB {
		lvlConfig := &lvldbopt.Options{}
		lvlDB, err := leveldb.OpenFile(fileName, lvlConfig)
		if err != nil {
			return nil, err
		}

		schema, err := validateLvlDBSchema(lvlDB, config)
		if err != nil {
			return nil, err
		}

		return &IndexDB{
			schema:    schema,
			indexFile: nil,
			db:        lvlDB,
			dbHelper:  levelDBHelper{0, dataCntQueryStr},
		}, nil
	}

	indexFile, err := storage.OpenYTFSIndexFile(fileName, config)
	if err != nil {
		return nil, err
	}

	err = validateDBSchema(indexFile.MetaData(), config)
	if err != nil {
		return nil, err
	}

	return &IndexDB{
		schema:    indexFile.MetaData(),
		indexFile: indexFile,
		db:        nil,
		dbHelper:  levelDBHelper{0, dataCntQueryStr},
	}, nil
}

// Get queries value corresponding to the input key.
func (db *IndexDB) Get(key ydcommon.IndexTableKey) (ydcommon.IndexTableValue, error) {
	if opt.UseLevelDB {
		buf, err := db.db.Get(key[:], nil)
		if err != nil {
			return 0, err
		}
		return ydcommon.IndexTableValue(binary.LittleEndian.Uint32(buf)), nil
	}
	return db.indexFile.Get(key)
}

// Put add new key value pair to db.
func (db *IndexDB) Put(key ydcommon.IndexTableKey, value ydcommon.IndexTableValue) error {
	if opt.UseLevelDB {
		valueBuf := make([]byte, 4)
		db.dbHelper.dataCnt++
		binary.LittleEndian.PutUint32(valueBuf, uint32(value))
		err := db.db.Put(key[:], valueBuf, nil)
		if err != nil {
			return err
		}

		binary.LittleEndian.PutUint32(valueBuf, db.dbHelper.dataCnt)
		return db.db.Put(db.dbHelper.dataCntQueryKey, valueBuf, nil)
	}
	return db.indexFile.Put(key, value)
}

// Close finishes all actions and close db connection.
func (db *IndexDB) Close() {
	if opt.UseLevelDB {
		db.db.Close()
		return
	}
	db.indexFile.Close()
}

// Reset finishes all actions and close db connection.
func (db *IndexDB) Reset() {
	db.indexFile.Format()
}

func validateDBSchema(meta *ydcommon.Header, opt *opt.Options) error {
	if meta.YtfsCapability != opt.TotalVolumn || meta.DataBlockSize != opt.DataBlockSize {
		return ErrConfigIndexMismatch
	}
	return nil
}

func validateLvlDBSchema(db *leveldb.DB, config *opt.Options) (*ydcommon.Header, error) {
	queryKey := []byte(config.YTFSTag)
	savedBuf, err := db.Get(queryKey, nil)
	if err == leveldb.ErrNotFound {
		newSchema := createIndexSchema(config)
		savedBuf, err = json.Marshal(newSchema)
		if err != nil {
			return nil, err
		}

		err = db.Put(queryKey, savedBuf, nil)
		if err != nil {
			return nil, err
		}

		err = db.Put(dataCntQueryStr, []byte{0, 0, 0, 0}, nil)
		if err != nil {
			return nil, err
		}
		return newSchema, nil
	} else if err != nil {
		return nil, err
	}

	savedMeta := ydcommon.Header{}
	err = json.Unmarshal(savedBuf, &savedMeta)
	if err != nil {
		return nil, err
	}

	err = validateDBSchema(&savedMeta, config)
	if err != nil {
		return nil, err
	}

	cntBuf, err := db.Get(dataCntQueryStr, nil)
	if err != nil {
		return nil, err
	}

	savedMeta.DataEndPoint = uint64(binary.LittleEndian.Uint32(cntBuf))
	return &savedMeta, nil
}

func createIndexSchema(config *opt.Options) *ydcommon.Header {
	m, n := config.IndexTableCols, config.IndexTableRows
	t, d, h := config.TotalVolumn, config.DataBlockSize, uint32(unsafe.Sizeof(ydcommon.Header{}))

	ytfsSize := uint64(0)
	for _, storageOption := range config.Storages {
		ytfsSize += storageOption.StorageVolume
	}

	// write header.
	return &ydcommon.Header{
		Tag:            [4]byte{'Y', 'T', 'F', 'S'},
		Version:        [4]byte{'0', '.', '0', '3'},
		YtfsCapability: t,
		YtfsSize:       ytfsSize,
		DataBlockSize:  d,
		RangeCapacity:  n,
		RangeCoverage:  m,
		HashOffset:     h,
		DataEndPoint:   0,
		RecycleOffset:  uint64(h) + (uint64(n)+1)*(uint64(m)*36+4),
		Reserved:       0xCDCDCDCDCDCDCDCD,
	}
}
