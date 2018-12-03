package tunnel

import (
	"bytes"
	"encoding/binary"
	"errors"
	LOG "github.com/vinllen/log4go"
	"io"
	"os"
	"path/filepath"
	"time"

	"mongoshake/collector/configure"
)

type FileReader struct {
	File     string
	dataFile *DataFile

	pipe      []chan *TMessage
	replayers []Replayer
}

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
			read := time.After(time.Second * time.Duration(conf.Options.ReadLogFileTime)) //xqw_time
			LOG.Debug("Start reading file" + tunnel.File)
			filepath.Walk(tunnel.File, func(path string, f os.FileInfo, err error) error {
				if f == nil {
					return err
				}
				if f.IsDir() {
					return nil
				}

				var file *os.File
				var er error
				if file, er = os.Open(path); er != nil {
					LOG.Critical("File tunnel reader open %s failed, %v", tunnel.File, err)
					return er
				}
				dataFile := &DataFile{filehandle: file}

				if fileHeader := dataFile.ReadHeader(); fileHeader.Magic != FILE_MAGIC_NUMBER || fileHeader.Protocol != FILE_PROTOCOL_NUMBER {
					LOG.Critical("File is not belong to mongoshake. magic header or protocol header is invalid")
					// check
					//if _, err := os.Stat(path); err == nil {
					//	fmt.Println("path exists 1", path)
					//} else {
					//	fmt.Println("path not exists ", path)
					//	err := os.MkdirAll(path, 0711)
					//
					//	if err != nil {
					//		log.Println("Error creating directory")
					//		log.Println(err)
					//	}
					//}
					//if er := os.Rename(path, "error/"+path); er != nil {
					//	LOG.Critical("copy fail name : %s", path)
					//	print(er)
					//}
					return errors.New("file magic number or protocol number is invalid")
				}

				go tunnel.read(dataFile.filehandle)

				os.RemoveAll(path)
				LOG.Debug("File read complete, delete " + path)

				return nil
			})
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
	defer close(filehandle)
	//defer os.RemoveAll(filehandle.)

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

func close(filehandle *os.File) error {
	LOG.Debug("close file" + filehandle.Name())
	return filehandle.Close()
}
