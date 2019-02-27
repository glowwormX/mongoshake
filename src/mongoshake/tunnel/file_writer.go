package tunnel

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mongoshake/collector/configure"

	LOG "github.com/vinllen/log4go"
)

const (
	OPEN_FILE_FLAGS = os.O_CREATE | os.O_RDWR | os.O_TRUNC
)

const (
	FILE_MAGIC_NUMBER    uint64 = 0xeeeeeeeeee201314
	FILE_PROTOCOL_NUMBER uint32 = 1
	BLOCK_HEADER_SIZE           = 20
)

var globalInitializer = int32(0)
var globalInitializerFile = int32(0)
var oplogMessage chan *TMessage

type FileWriter struct {
	// local file folder path
	Local string
	done  chan struct{}
	wg    sync.WaitGroup

	// data file header
	fileHeader *FileHeader
	// data file handle
	dataFile       *DataFile
	changeFileLock sync.Mutex

	logs uint64
}

/**
 *  File Structure
 *
 *  |----- Header ------|------ OplogBlock ------|------ OplogBlock --------| ......
 *  |<--- 32bytes ---->|
 *
 */
type FileHeader struct {
	Magic    uint64
	Protocol uint32
	Checksum uint32
	Reserved [16]byte
}

type DataFile struct {
	filehandle *os.File
}

func (dataFile *DataFile) WriteHeader() {
	fileHeader := new(FileHeader)
	fileHeader.Magic = FILE_MAGIC_NUMBER
	fileHeader.Protocol = FILE_PROTOCOL_NUMBER

	buffer := bytes.Buffer{}
	binary.Write(&buffer, binary.BigEndian, fileHeader.Magic)
	binary.Write(&buffer, binary.BigEndian, fileHeader.Protocol)
	binary.Write(&buffer, binary.BigEndian, fileHeader.Checksum)
	binary.Write(&buffer, binary.BigEndian, fileHeader.Reserved)

	dataFile.filehandle.Write(buffer.Bytes())
	dataFile.filehandle.Sync()
	dataFile.filehandle.Seek(32, 0)
}

func (dataFile *DataFile) ReadHeader() *FileHeader {
	fileHeader := &FileHeader{}
	header := [32]byte{}

	io.ReadFull(dataFile.filehandle, header[:])
	buffer := bytes.NewBuffer(header[:])

	binary.Read(buffer, binary.BigEndian, &fileHeader.Magic)
	binary.Read(buffer, binary.BigEndian, &fileHeader.Protocol)
	binary.Read(buffer, binary.BigEndian, &fileHeader.Checksum)
	binary.Read(buffer, binary.BigEndian, &fileHeader.Reserved)

	return fileHeader
}

func (tunnel *FileWriter) Send(message *WMessage) int64 {
	if message.Tag&MsgProbe == 0 {
		oplogMessage <- message.TMessage
	}
	return 0
}

func (tunnel *FileWriter) SyncToDisk() {
	buffer := &bytes.Buffer{}

	for {
		select {
		case message := <-oplogMessage:
			// oplogs array
			for _, log := range message.RawLogs {
				tunnel.logs++
				binary.Write(buffer, binary.BigEndian, uint32(len(log)))
				binary.Write(buffer, binary.BigEndian, log)
			}

			tag := message.Tag | MsgPersistent | MsgStorageBackend

			headerBuffer := &bytes.Buffer{}
			binary.Write(headerBuffer, binary.BigEndian, message.Checksum)
			binary.Write(headerBuffer, binary.BigEndian, tag)
			binary.Write(headerBuffer, binary.BigEndian, message.Shard)
			binary.Write(headerBuffer, binary.BigEndian, message.Compress)
			binary.Write(headerBuffer, binary.BigEndian, uint32(0xeeeeeeee))
			binary.Write(headerBuffer, binary.BigEndian, uint32(buffer.Len()))
			//
			tunnel.changeFileLock.Lock()
			tunnel.dataFile.filehandle.Write(headerBuffer.Bytes())
			tunnel.dataFile.filehandle.Write(buffer.Bytes())
			tunnel.changeFileLock.Unlock()

			buffer.Reset()
		case <-time.After(time.Millisecond * 1000):
			LOG.Info("File tunnel sync flush. total oplogs %d", tunnel.logs)
			tunnel.dataFile.filehandle.Sync()
		}
	}
}

func _Open(path string) (*os.File, bool) {
	if file, err := os.OpenFile(path, OPEN_FILE_FLAGS, os.ModePerm); err == nil {
		return file, true
	}
	LOG.Critical("File tunnel create data file failed")
	return nil, false
}

func (tunnel *FileWriter) PrepareOld() bool {
	if atomic.CompareAndSwapInt32(&globalInitializer, 0, 1) {
		if file, ok := _Open(tunnel.Local); ok {
			tunnel.dataFile = &DataFile{filehandle: file}
		} else {
			LOG.Critical("File tunnel open failed")
			return false
		}

		if info, err := os.Stat(tunnel.Local); err != nil || info.IsDir() {
			LOG.Critical("File tunnel check path failed. %v", err)
			return false
		}
		tunnel.dataFile.WriteHeader()

		oplogMessage = make(chan *TMessage, 8192)

		go tunnel.SyncToDisk()
	}

	return true
}

func (tunnel *FileWriter) replaceNewFile() bool {
	var dataFile *DataFile
	oldFile := tunnel.dataFile
	if file, ok := _Open(tunnel.Local); ok {
		dataFile = &DataFile{filehandle: file}
	} else {
		LOG.Critical("File tunnel open failed")
		return false
	}

	if info, err := os.Stat(tunnel.Local); err != nil || info.IsDir() {
		LOG.Critical("File tunnel check path failed. %v", err)
		return false
	}
	dataFile.WriteHeader()
	// Replace write file
	tunnel.changeFileLock.Lock()
	tunnel.dataFile = dataFile
	tunnel.changeFileLock.Unlock()

	oldFile.filehandle.Close()
	return true
}

func (tunnel *FileWriter) AckRequired() bool {
	return false
}

func (tunnel *FileWriter) ParsedLogsRequired() bool {
	return false
}

func (tunnel *FileWriter) Prepare() bool {
	tunnel.Local = strconv.FormatInt(time.Now().Unix(), 10) + conf.Options.TunnelAddress[0]
	tunnel.PrepareOld()

	if atomic.CompareAndSwapInt32(&globalInitializerFile, 0, 1) {
		go tunnel.StartNext(tunnel.Local, "")
	}
	return true
}

func (tunnel *FileWriter) StartNext(lastFile string, lastDir string) {
	if conf.Options.CopyLogFileTime > 0 {
		select {
		case <-time.Tick(time.Second * time.Duration(conf.Options.CopyLogFileTime)): //copy time
			tunnel.Local = strconv.FormatInt(time.Now().Unix(), 10) + conf.Options.TunnelAddress[0]
			tunnel.replaceNewFile()
			moveToOtherDir(lastFile, conf.Options.CopyLogFilePath+"/")
		}
		go tunnel.StartNext(tunnel.Local, "")
	} else {
		bakDir := time.Now().Format("2006-01-02T15:04:05")
		tunnel.Local = bakDir + conf.Options.TunnelAddress[0]
		tunnel.replaceNewFile()
		mongodump(bakDir)
		moveToOtherDir(lastFile, conf.Options.CopyLogFilePath+"/"+lastDir+"/")

		now := time.Now()
		// 计算下一个零点
		//next := now.Add(time.Minute * 1)
		//next = time.Date(next.Year(), next.Month(), next.Day(), next.Hour(), next.Minute(), 0, 0, next.Location())
		next := now.Add(time.Hour * 24)
		next = time.Date(next.Year(), next.Month(), next.Day(), 0, 0, 0, 0, next.Location())
		t := time.NewTimer(next.Sub(now))
		LOG.Info("mongodump next time, %s", next.Format("2006-01-02T15:04:05"))
		<-t.C
		go tunnel.StartNext(tunnel.Local, bakDir)
	}
}

func mongodump(bakDir string) {
	var err error
	var cmd *exec.Cmd

	// 执行单个shell命令时, 直接运行即可
	//var dump = "/usr/local/software/mongodb/bin/mongodump -h 192.168.1.78 --port 2001 -d mbxt -o /home/hlkj/Documents/mongobak/test"
	var dump = conf.Options.MongoDumpCmd + bakDir
	cmd = exec.Command("/bin/sh", "-c", dump)
	if _, err = cmd.Output(); err != nil {
		LOG.Critical("mongodump failed,err: %v", err)
	} else {
		LOG.Info("mongodump success,cmd: %s", dump)
	}
}

func moveToOtherDir(path string, dir string) {
	var dirM, fileName string
	pos := strings.LastIndex(path, "/")
	if pos == -1 {
		dirM = dir
		fileName = path
	} else {
		dirM = path[0:pos] + dir
		fileName = path[pos+1:]
	}
	if er := os.Rename(path, dirM+fileName); er != nil {
		if err := os.MkdirAll(dirM, 0711); err == nil {
			if er := os.Rename(path, dirM+fileName); er != nil {
				LOG.Critical("file moved failed, name : %s", path)
			}
		}
	}
}
