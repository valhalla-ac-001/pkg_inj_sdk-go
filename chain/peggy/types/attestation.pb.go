// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: injective/peggy/v1beta1/attestation.proto

package types

import (
	fmt "fmt"
	github_com_cosmos_cosmos_sdk_types "github.com/cosmos/cosmos-sdk/types"
	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

// ClaimType is the cosmos type of an event from the counterpart chain that can
// be handled
type ClaimType int32

const (
	CLAIM_TYPE_UNKNOWN  ClaimType = 0
	CLAIM_TYPE_DEPOSIT  ClaimType = 1
	CLAIM_TYPE_WITHDRAW ClaimType = 2
)

var ClaimType_name = map[int32]string{
	0: "CLAIM_TYPE_UNKNOWN",
	1: "CLAIM_TYPE_DEPOSIT",
	2: "CLAIM_TYPE_WITHDRAW",
}

var ClaimType_value = map[string]int32{
	"CLAIM_TYPE_UNKNOWN":  0,
	"CLAIM_TYPE_DEPOSIT":  1,
	"CLAIM_TYPE_WITHDRAW": 2,
}

func (x ClaimType) String() string {
	return proto.EnumName(ClaimType_name, int32(x))
}

func (ClaimType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_103514c8073046b6, []int{0}
}

// Attestation is an aggregate of `claims` that eventually becomes `observed` by
// all orchestrators
// EVENT_NONCE:
// EventNonce a nonce provided by the peggy contract that is unique per event fired
// These event nonces must be relayed in order. This is a correctness issue,
// if relaying out of order transaction replay attacks become possible
// OBSERVED:
// Observed indicates that >67% of validators have attested to the event,
// and that the event should be executed by the peggy state machine
//
// The actual content of the claims is passed in with the transaction making the claim
// and then passed through the call stack alongside the attestation while it is processed
// the key in which the attestation is stored is keyed on the exact details of the claim
// but there is no reason to store those exact details becuause the next message sender
// will kindly provide you with them.
type Attestation struct {
	EventNonce uint64   `protobuf:"varint,1,opt,name=event_nonce,json=eventNonce,proto3" json:"event_nonce,omitempty"`
	Observed   bool     `protobuf:"varint,2,opt,name=observed,proto3" json:"observed,omitempty"`
	Votes      []string `protobuf:"bytes,3,rep,name=votes,proto3" json:"votes,omitempty"`
}

func (m *Attestation) Reset()         { *m = Attestation{} }
func (m *Attestation) String() string { return proto.CompactTextString(m) }
func (*Attestation) ProtoMessage()    {}
func (*Attestation) Descriptor() ([]byte, []int) {
	return fileDescriptor_103514c8073046b6, []int{0}
}
func (m *Attestation) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Attestation) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Attestation.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Attestation) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Attestation.Merge(m, src)
}
func (m *Attestation) XXX_Size() int {
	return m.Size()
}
func (m *Attestation) XXX_DiscardUnknown() {
	xxx_messageInfo_Attestation.DiscardUnknown(m)
}

var xxx_messageInfo_Attestation proto.InternalMessageInfo

func (m *Attestation) GetEventNonce() uint64 {
	if m != nil {
		return m.EventNonce
	}
	return 0
}

func (m *Attestation) GetObserved() bool {
	if m != nil {
		return m.Observed
	}
	return false
}

func (m *Attestation) GetVotes() []string {
	if m != nil {
		return m.Votes
	}
	return nil
}

// ERC20Token unique identifier for an Ethereum ERC20 token.
// CONTRACT:
// The contract address on ETH of the token (note: developers should look up
// the token symbol using the address on ETH to display for UI)
type ERC20Token struct {
	Amount   github_com_cosmos_cosmos_sdk_types.Int `protobuf:"bytes,1,opt,name=amount,proto3,customtype=github.com/cosmos/cosmos-sdk/types.Int" json:"amount"`
	Contract string                                 `protobuf:"bytes,2,opt,name=contract,proto3" json:"contract,omitempty"`
}

func (m *ERC20Token) Reset()         { *m = ERC20Token{} }
func (m *ERC20Token) String() string { return proto.CompactTextString(m) }
func (*ERC20Token) ProtoMessage()    {}
func (*ERC20Token) Descriptor() ([]byte, []int) {
	return fileDescriptor_103514c8073046b6, []int{1}
}
func (m *ERC20Token) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ERC20Token) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ERC20Token.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ERC20Token) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ERC20Token.Merge(m, src)
}
func (m *ERC20Token) XXX_Size() int {
	return m.Size()
}
func (m *ERC20Token) XXX_DiscardUnknown() {
	xxx_messageInfo_ERC20Token.DiscardUnknown(m)
}

var xxx_messageInfo_ERC20Token proto.InternalMessageInfo

func (m *ERC20Token) GetContract() string {
	if m != nil {
		return m.Contract
	}
	return ""
}

func init() {
	proto.RegisterEnum("injective.peggy.v1beta1.ClaimType", ClaimType_name, ClaimType_value)
	proto.RegisterType((*Attestation)(nil), "injective.peggy.v1beta1.Attestation")
	proto.RegisterType((*ERC20Token)(nil), "injective.peggy.v1beta1.ERC20Token")
}

func init() {
	proto.RegisterFile("injective/peggy/v1beta1/attestation.proto", fileDescriptor_103514c8073046b6)
}

var fileDescriptor_103514c8073046b6 = []byte{
	// 409 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x92, 0xc1, 0x4e, 0xdb, 0x40,
	0x10, 0x86, 0xbd, 0x40, 0x11, 0x5e, 0x2e, 0xd1, 0x16, 0x95, 0xc8, 0x07, 0xc7, 0xe2, 0x50, 0xa5,
	0x95, 0xf0, 0x42, 0xfb, 0x04, 0x21, 0xa4, 0xaa, 0x05, 0x35, 0xc8, 0x75, 0x15, 0xb5, 0x97, 0x74,
	0xed, 0x8c, 0x8c, 0x0b, 0xde, 0xb1, 0xbc, 0x1b, 0x4b, 0x79, 0x83, 0x8a, 0x53, 0x5f, 0x80, 0x13,
	0x2f, 0xc3, 0x91, 0x63, 0xd5, 0x03, 0xaa, 0x92, 0x17, 0xa9, 0x62, 0x5c, 0x37, 0x2a, 0x39, 0xed,
	0xfe, 0xf3, 0x7f, 0x3b, 0x1a, 0xcd, 0xbf, 0xf4, 0x55, 0x2a, 0xbf, 0x41, 0xac, 0xd3, 0x12, 0x78,
	0x0e, 0x49, 0x32, 0xe5, 0xe5, 0x61, 0x04, 0x5a, 0x1c, 0x72, 0xa1, 0x35, 0x28, 0x2d, 0x74, 0x8a,
	0xd2, 0xcd, 0x0b, 0xd4, 0xc8, 0x76, 0x1b, 0xd4, 0xad, 0x50, 0xb7, 0x46, 0xad, 0x9d, 0x04, 0x13,
	0xac, 0x18, 0xbe, 0xb8, 0x3d, 0xe2, 0x7b, 0x5f, 0xe9, 0x76, 0xef, 0x5f, 0x0f, 0xd6, 0xa1, 0xdb,
	0x50, 0x82, 0xd4, 0x23, 0x89, 0x32, 0x86, 0x36, 0x71, 0x48, 0x77, 0x23, 0xa0, 0x55, 0xc9, 0x5f,
	0x54, 0x98, 0x45, 0xb7, 0x30, 0x52, 0x50, 0x94, 0x30, 0x6e, 0xaf, 0x39, 0xa4, 0xbb, 0x15, 0x34,
	0x9a, 0xed, 0xd0, 0x67, 0x25, 0x6a, 0x50, 0xed, 0x75, 0x67, 0xbd, 0x6b, 0x06, 0x8f, 0x62, 0x2f,
	0xa7, 0x74, 0x10, 0xf4, 0xdf, 0x1c, 0x84, 0x78, 0x09, 0x92, 0xbd, 0xa3, 0x9b, 0x22, 0xc3, 0x89,
	0xd4, 0x55, 0x6f, 0xf3, 0xc8, 0xbd, 0x7b, 0xe8, 0x18, 0xbf, 0x1e, 0x3a, 0x2f, 0x93, 0x54, 0x5f,
	0x4c, 0x22, 0x37, 0xc6, 0x8c, 0xc7, 0xa8, 0x32, 0x54, 0xf5, 0xb1, 0xaf, 0xc6, 0x97, 0x5c, 0x4f,
	0x73, 0x50, 0xae, 0x27, 0x75, 0x50, 0xbf, 0x5e, 0xcc, 0x11, 0xa3, 0xd4, 0x85, 0x88, 0x75, 0x35,
	0x87, 0x19, 0x34, 0xfa, 0xf5, 0x2d, 0xa1, 0x66, 0xff, 0x4a, 0xa4, 0x59, 0x38, 0xcd, 0x81, 0xb9,
	0x94, 0xf5, 0x4f, 0x7b, 0xde, 0x87, 0x51, 0xf8, 0xf9, 0x7c, 0x30, 0xfa, 0xe4, 0x9f, 0xf8, 0x67,
	0x43, 0xbf, 0x65, 0x58, 0x2f, 0xae, 0x6f, 0x9c, 0x15, 0xce, 0x7f, 0xfc, 0xf1, 0xe0, 0xfc, 0xec,
	0xa3, 0x17, 0xb6, 0xc8, 0x13, 0xbe, 0x76, 0xd8, 0x01, 0x7d, 0xbe, 0x54, 0x1d, 0x7a, 0xe1, 0xfb,
	0xe3, 0xa0, 0x37, 0x6c, 0xad, 0x59, 0xbb, 0xd7, 0x37, 0xce, 0x2a, 0xcb, 0xda, 0xf8, 0x7e, 0x6b,
	0x1b, 0x47, 0x70, 0x37, 0xb3, 0xc9, 0xfd, 0xcc, 0x26, 0xbf, 0x67, 0x36, 0xf9, 0x31, 0xb7, 0x8d,
	0xfb, 0xb9, 0x6d, 0xfc, 0x9c, 0xdb, 0xc6, 0x97, 0x93, 0xa5, 0x5d, 0x78, 0x7f, 0xd3, 0x3c, 0x15,
	0x91, 0xe2, 0x4d, 0xb6, 0xfb, 0x31, 0x16, 0xb0, 0x2c, 0x2f, 0x44, 0x2a, 0x79, 0x86, 0xe3, 0xc9,
	0x15, 0xa8, 0xfa, 0x8f, 0x54, 0x4b, 0x8b, 0x36, 0xab, 0x9c, 0xdf, 0xfe, 0x09, 0x00, 0x00, 0xff,
	0xff, 0x19, 0x8a, 0x07, 0x59, 0x43, 0x02, 0x00, 0x00,
}

func (m *Attestation) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Attestation) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Attestation) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Votes) > 0 {
		for iNdEx := len(m.Votes) - 1; iNdEx >= 0; iNdEx-- {
			i -= len(m.Votes[iNdEx])
			copy(dAtA[i:], m.Votes[iNdEx])
			i = encodeVarintAttestation(dAtA, i, uint64(len(m.Votes[iNdEx])))
			i--
			dAtA[i] = 0x1a
		}
	}
	if m.Observed {
		i--
		if m.Observed {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x10
	}
	if m.EventNonce != 0 {
		i = encodeVarintAttestation(dAtA, i, uint64(m.EventNonce))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *ERC20Token) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ERC20Token) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ERC20Token) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Contract) > 0 {
		i -= len(m.Contract)
		copy(dAtA[i:], m.Contract)
		i = encodeVarintAttestation(dAtA, i, uint64(len(m.Contract)))
		i--
		dAtA[i] = 0x12
	}
	{
		size := m.Amount.Size()
		i -= size
		if _, err := m.Amount.MarshalTo(dAtA[i:]); err != nil {
			return 0, err
		}
		i = encodeVarintAttestation(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0xa
	return len(dAtA) - i, nil
}

func encodeVarintAttestation(dAtA []byte, offset int, v uint64) int {
	offset -= sovAttestation(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *Attestation) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.EventNonce != 0 {
		n += 1 + sovAttestation(uint64(m.EventNonce))
	}
	if m.Observed {
		n += 2
	}
	if len(m.Votes) > 0 {
		for _, s := range m.Votes {
			l = len(s)
			n += 1 + l + sovAttestation(uint64(l))
		}
	}
	return n
}

func (m *ERC20Token) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = m.Amount.Size()
	n += 1 + l + sovAttestation(uint64(l))
	l = len(m.Contract)
	if l > 0 {
		n += 1 + l + sovAttestation(uint64(l))
	}
	return n
}

func sovAttestation(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozAttestation(x uint64) (n int) {
	return sovAttestation(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Attestation) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowAttestation
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Attestation: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Attestation: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field EventNonce", wireType)
			}
			m.EventNonce = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAttestation
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.EventNonce |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Observed", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAttestation
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Observed = bool(v != 0)
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Votes", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAttestation
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthAttestation
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthAttestation
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Votes = append(m.Votes, string(dAtA[iNdEx:postIndex]))
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipAttestation(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthAttestation
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthAttestation
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ERC20Token) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowAttestation
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ERC20Token: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ERC20Token: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Amount", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAttestation
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthAttestation
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthAttestation
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.Amount.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Contract", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAttestation
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthAttestation
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthAttestation
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Contract = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipAttestation(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthAttestation
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthAttestation
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipAttestation(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowAttestation
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowAttestation
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowAttestation
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthAttestation
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupAttestation
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthAttestation
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthAttestation        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowAttestation          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupAttestation = fmt.Errorf("proto: unexpected end of group")
)