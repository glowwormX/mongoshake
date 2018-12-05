package tunnel

import (
	"bytes"
	"encoding/binary"
	LOG "github.com/vinllen/log4go"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"time"

	"mongoshake/collector/configure"
)

type FileReader struct {
	File     string
	dataFile *DataFile

	pipe      []chan *TMessage
	replayers []Replayer
}

type FileInfoList []os.FileInfo

func (fs FileInfoList) Len() int { return len(fs) }

func (fs FileInfoList) Less(i, j int) bool {
	return fs[i].Name() > fs[j].Name()
}

func (fs FileInfoList) Swap(i, j int) { fs[i], fs[j] = fs[j], fs[i] }

func (tunnel *FileReader) Link(relativeReplayer []Replayer) error {
	tunnel.replayers = relativeReplayer
	tunnel.pipe = make([]chan *TMessage, 0)
	for i := 0; i != len(tunnel.replayers); i++ {
		ch := make(chan *TMessage)
		tunnel.pipe = append(tunnel.pipe, ch)
		go tunnel.consume(ch)
	}
	go func() {
		for {
			read := time.After(time.Second * time.Duration(conf.Options.ReadLogFileTime)) //read time
			LOG.Debug("Start reading file :" + tunnel.File)
			if files, er := ioutil.ReadDir(tunnel.File); er == nil {
				var fs FileInfoList = files
				sort.Sort(fs)
				for _, f := range files {
					if f == nil {
						continue
					}
					if f.IsDir() {
						continue
					}
					var file *os.File
					var er error
					if file, er = os.Open(tunnel.File + "/" + f.Name()); er != nil {
						LOG.Critical("File tunnel reader open %s failed, %v", tunnel.File, er)
					}
					dataFile := &DataFile{filehandle: file}

					if fileHeader := dataFile.ReadHeader(); fileHeader.Magic != FILE_MAGIC_NUMBER || fileHeader.Protocol != FILE_PROTOCOL_NUMBER {
						LOG.Critical("File is not belong to mongoshake. magic header or protocol header is invalid")
						moveToOtherDir(tunnel.File+"/"+f.Name(), "/error/")
						continue
					}
					tunnel.read(dataFile.filehandle)
				}
			}
			<-read
		}
	}()

	return nil
}

func (tunnel *FileReader) consume(pipe <-chan *TMessage) {
	seqKey := 1
	for msg := range pipe {
		// hash corresponding replayer
		seqKey++
		switch tunnel.replayers[msg.Shard].Sync(msg, func(context *TMessage, seq int) func() {
			return func() {
				LOG.Info("Sync tunnel message successful, signature: %d, %d", context.Checksum, seq)
			}
		}(msg, seqKey)) {
		case ReplyChecksumInvalid:
			fallthrough
		case ReplyRetransmission:
			fallthrough
		case ReplyCompressorNotSupported:
			fallthrough
		case ReplyNetworkOpFail:
			LOG.Warn("File tunnel rejected by replayer-%d", msg.Shard)
		case ReplyError:
			fallthrough
		case ReplyServerFault:
			LOG.Critical("File tunnel handle server fault")
		}
	}
}

func (tunnel *FileReader) read(filehandle *os.File) {
	defer closeFile(filehandle)
	defer deleteFile(filehandle)

	bufferedReader := filehandle
	bits := make([]byte, 4, 4)
	totalLogs := 0
	for {
		message := new(TMessage)

		// for checksum multi read() is acceptable, the underlaying reader is Buffered
		if n, err := io.ReadFull(bufferedReader, bits); n != len(bits) || err != nil {
			break
		}
		message.Checksum = binary.BigEndian.Uint32(bits[:])
		// for tag
		io.ReadFull(bufferedReader, bits)
		message.Tag = binary.BigEndian.Uint32(bits[:])
		// for shard
		io.ReadFull(bufferedReader, bits)
		message.Shard = binary.BigEndian.Uint32(bits[:])
		// for compress
		io.ReadFull(bufferedReader, bits)
		message.Compress = binary.BigEndian.Uint32(bits[:])
		// for 0xeeeeeeee
		io.ReadFull(bufferedReader, bits)
		if !bytes.Equal(bits, []byte{0xee, 0xee, 0xee, 0xee}) {
			LOG.Critical("File oplog block magic is not 0xeeeeeeee. found 0x%x", bits)
			break
		}
		io.ReadFull(bufferedReader, bits)
		blockRemained := binary.BigEndian.Uint32(bits)

		logs := [][]byte{}
		for blockRemained > 0 {
			// oplog entry length
			io.ReadFull(bufferedReader, bits[:])
			oplogLength := binary.BigEndian.Uint32(bits[:])
			log := make([]byte, oplogLength, oplogLength)
			if _, err := io.ReadFull(bufferedReader, log); err == io.EOF {
				break
			}

			logs = append(logs, log)
			// header + body
			blockRemained -= (4 + oplogLength)
			totalLogs++
		}
		message.RawLogs = logs

		if message.Shard < 0 {
			LOG.Warn("Oplog hashed value is bad negative")
			break
		}
		message.Tag |= MsgRetransmission

		// resharding
		if message.Shard >= uint32(len(tunnel.pipe)) {
			message.Shard %= uint32(len(tunnel.pipe))
		}
		tunnel.pipe[message.Shard] <- message
		LOG.Info("File tunnel reader extract oplogs with shard[%d], compressor[%d], count (%d)", message.Shard, message.Compress, len(message.RawLogs))
	}
	LOG.Info("File tunnel reader complete. total oplogs %d", totalLogs)

}

func deleteFile(filehandle *os.File) error {
	LOG.Debug("File read complete, delete " + filehandle.Name())
	return os.RemoveAll(filehandle.Name())
}

func closeFile(filehandle *os.File) error {
	LOG.Debug("close file" + filehandle.Name())
	return filehandle.Close()
}
