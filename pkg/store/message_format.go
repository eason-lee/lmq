package store

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	pb "github.com/eason-lee/lmq/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// 消息格式常量
const (
	// 消息魔数，用于标识消息格式
	MessageMagic byte = 0x4C // 'L' for LMQ

	// 消息版本号
	MessageVersion byte = 0x01

	// 消息头部固定字段大小（魔数+版本号+属性+CRC32）
	MessageHeaderFixedSize = 7

	// 消息属性标志位
	AttrCompressed byte = 0x01 // 消息是否压缩
	AttrBatched    byte = 0x02 // 消息是否为批量消息的一部分
)

// 压缩类型
type CompressionType byte

const (
	NoCompression    CompressionType = 0
	GzipCompression  CompressionType = 1
	SnappyCompression CompressionType = 2
	LZ4Compression   CompressionType = 3
)

// MessageHeader 消息头部
type MessageHeader struct {
	Magic       byte            // 消息魔数
	Version     byte            // 版本号
	Attributes  byte            // 属性标志
	CRC         uint32          // CRC32校验和
	ID          string          // 消息ID
	Topic       string          // 主题
	Partition   int32           // 分区ID
	Timestamp   int64           // 时间戳
	Compression CompressionType // 压缩类型
}

// EncodeMessage 将消息编码为二进制格式
func EncodeMessage(msg *pb.Message) ([]byte, error) {
	// 1. 准备消息头部
	header := MessageHeader{
		Magic:     MessageMagic,
		Version:   MessageVersion,
		Attributes: 0,
		ID:        msg.Id,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Timestamp: msg.Timestamp.Seconds,
	}

	// 2. 序列化消息体
	body := msg.Body

	// 3. 计算头部大小
	idLen := len(header.ID)
	topicLen := len(header.Topic)
	headerSize := MessageHeaderFixedSize + 1 + idLen + 1 + topicLen + 12 // 12 = 8(timestamp) + 4(partition)

	// 4. 分配缓冲区
	totalSize := 4 + 1 + headerSize + len(body) // 4(消息长度) + 1(头部长度) + 头部 + 消息体
	buf := make([]byte, totalSize)

	// 5. 写入消息长度（不包括自身的4字节）
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalSize-4))

	// 6. 写入头部长度
	buf[4] = byte(headerSize)

	// 7. 写入头部固定字段
	pos := 5
	buf[pos] = header.Magic
	pos++
	buf[pos] = header.Version
	pos++
	buf[pos] = header.Attributes
	pos++

	// 8. 预留CRC32位置
	crcPos := pos
	pos += 4

	// 9. 写入ID
	buf[pos] = byte(idLen)
	pos++
	copy(buf[pos:pos+idLen], header.ID)
	pos += idLen

	// 10. 写入Topic
	buf[pos] = byte(topicLen)
	pos++
	copy(buf[pos:pos+topicLen], header.Topic)
	pos += topicLen

	// 11. 写入时间戳
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(header.Timestamp))
	pos += 8

	// 12. 写入分区ID
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(header.Partition))
	pos += 4

	// 13. 写入消息体
	copy(buf[pos:], body)

	// 14. 计算并写入CRC32
	crc := crc32.ChecksumIEEE(buf[crcPos+4:])
	binary.BigEndian.PutUint32(buf[crcPos:crcPos+4], crc)

	return buf, nil
}

// DecodeMessage 从二进制格式解码消息
func DecodeMessage(data []byte) (*pb.Message, error) {
	// 1. 检查数据长度
	if len(data) < 5 { // 至少需要4字节消息长度 + 1字节头部长度
		return nil, fmt.Errorf("数据长度不足")
	}

	// 2. 读取消息长度和头部长度
	msgLen := binary.BigEndian.Uint32(data[0:4])
	headerLen := int(data[4])

	// 3. 检查数据完整性
	if len(data) < int(msgLen)+4 {
		return nil, fmt.Errorf("消息数据不完整")
	}

	// 4. 检查头部长度
	if headerLen < MessageHeaderFixedSize {
		return nil, fmt.Errorf("头部长度不足")
	}

	// 5. 读取头部
	pos := 5
	magic := data[pos]
	pos++
	version := data[pos]
	pos++
	attributes := data[pos]
	pos++

	// 6. 读取CRC32
	crc := binary.BigEndian.Uint32(data[pos:pos+4])
	pos += 4

	// 7. 验证CRC32
	expectedCRC := crc32.ChecksumIEEE(data[pos:])
	if crc != expectedCRC {
		return nil, fmt.Errorf("CRC校验失败")
	}

	// 8. 检查魔数和版本
	if magic != MessageMagic {
		return nil, fmt.Errorf("无效的消息魔数")
	}
	if version != MessageVersion {
		return nil, fmt.Errorf("不支持的消息版本")
	}

	// 9. 读取ID
	idLen := int(data[pos])
	pos++
	if pos+idLen > len(data) {
		return nil, fmt.Errorf("数据长度不足，无法读取ID")
	}
	id := string(data[pos:pos+idLen])
	pos += idLen

	// 10. 读取Topic
	topicLen := int(data[pos])
	pos++
	if pos+topicLen > len(data) {
		return nil, fmt.Errorf("数据长度不足，无法读取Topic")
	}
	topic := string(data[pos:pos+topicLen])
	pos += topicLen

	// 11. 读取时间戳
	if pos+8 > len(data) {
		return nil, fmt.Errorf("数据长度不足，无法读取时间戳")
	}
	timestamp := int64(binary.BigEndian.Uint64(data[pos:pos+8]))
	pos += 8

	// 12. 读取分区ID
	if pos+4 > len(data) {
		return nil, fmt.Errorf("数据长度不足，无法读取分区ID")
	}
	partition := int32(binary.BigEndian.Uint32(data[pos:pos+4]))
	pos += 4

	// 13. 读取消息体
	body := data[pos:]

	// 14. 检查是否需要解压缩
	compressed := (attributes & AttrCompressed) != 0
	if compressed {
		// TODO: 实现解压缩逻辑
		return nil, fmt.Errorf("暂不支持压缩消息")
	}

	// 15. 构建消息对象
	msg := &pb.Message{
		Id:        id,
		Topic:     topic,
		Partition: partition,
		Timestamp: &timestamppb.Timestamp{Seconds: timestamp},
		Body:      body,
	}

	return msg, nil
}

// EncodeBatchMessages 批量编码消息
func EncodeBatchMessages(messages []*pb.Message) ([]byte, error) {
	// TODO: 实现批量消息编码
	// 1. 为每条消息设置批处理属性
	// 2. 编码每条消息
	// 3. 合并所有消息数据
	return nil, fmt.Errorf("批量消息编码功能尚未实现")
}

// DecodeBatchMessages 批量解码消息
func DecodeBatchMessages(data []byte) ([]*pb.Message, error) {
	// TODO: 实现批量消息解码
	// 1. 解析批量消息头部
	// 2. 解码每条消息
	// 3. 返回消息列表
	return nil, fmt.Errorf("批量消息解码功能尚未实现")
}