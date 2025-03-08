package store

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/eason-lee/lmq/pkg/protocol"
	pb "github.com/eason-lee/lmq/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// serializeMessage 将消息序列化为二进制格式
func serializeMessage(msg *protocol.Message) ([]byte, error) {
	// 计算所需的总字节数
	// 格式:
	// - 4字节: ID长度
	// - ID字节: ID内容
	// - 4字节: Topic长度
	// - Topic字节: Topic内容
	// - 8字节: Timestamp
	// - 4字节: Body长度
	// - Body字节: Body内容
	idBytes := []byte(msg.Id)
	topicBytes := []byte(msg.Topic)

	totalSize := 4 + len(idBytes) + 4 + len(topicBytes) + 8 + 4 + len(msg.Body)
	buf := make([]byte, totalSize)

	offset := 0

	// 写入ID
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(idBytes)))
	offset += 4
	copy(buf[offset:offset+len(idBytes)], idBytes)
	offset += len(idBytes)

	// 写入Topic
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(topicBytes)))
	offset += 4
	copy(buf[offset:offset+len(topicBytes)], topicBytes)
	offset += len(topicBytes)

	// 写入Timestamp
	binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(msg.Timestamp.Seconds))
	offset += 8

	// 写入Body
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(msg.Body)))
	offset += 4
	copy(buf[offset:], msg.Body)

	return buf, nil
}

// deserializeMessage 从二进制格式反序列化消息
func deserializeMessage(data []byte) (*protocol.Message, error) {
	if len(data) < 12 { // 至少需要包含ID长度、Topic长度和Timestamp
		return nil, fmt.Errorf("数据长度不足")
	}

	offset := 0

	// 读取ID
	idLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	if offset+int(idLen) > len(data) {
		return nil, fmt.Errorf("数据长度不足，无法读取ID")
	}
	id := string(data[offset : offset+int(idLen)])
	offset += int(idLen)

	// 读取Topic
	if offset+4 > len(data) {
		return nil, fmt.Errorf("数据长度不足，无法读取Topic长度")
	}
	topicLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	if offset+int(topicLen) > len(data) {
		return nil, fmt.Errorf("数据长度不足，无法读取Topic")
	}
	topic := string(data[offset : offset+int(topicLen)])
	offset += int(topicLen)

	// 读取Timestamp
	if offset+8 > len(data) {
		return nil, fmt.Errorf("数据长度不足，无法读取Timestamp")
	}
	timestamp := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	// 读取Body
	if offset+4 > len(data) {
		return nil, fmt.Errorf("数据长度不足，无法读取Body长度")
	}
	bodyLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	if offset+int(bodyLen) > len(data) {
		return nil, fmt.Errorf("数据长度不足，无法读取Body")
	}
	body := make([]byte, bodyLen)
	return &protocol.Message{
		Message: &pb.Message{
			Id:        id,
			Topic:     topic,
			Body:      body,
			Timestamp: timestamppb.New(time.Unix(timestamp, 0)),
		},
	}, nil
}
