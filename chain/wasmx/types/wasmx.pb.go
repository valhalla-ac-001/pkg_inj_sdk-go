// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: injective/wasmx/v1/wasmx.proto

package types

import (
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"

	_ "github.com/cosmos/gogoproto/gogoproto"
	proto "github.com/cosmos/gogoproto/proto"
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

type Params struct {
	// Set the status to active to indicate that contracts can be executed in
	// begin blocker.
	IsExecutionEnabled bool `protobuf:"varint,1,opt,name=is_execution_enabled,json=isExecutionEnabled,proto3" json:"is_execution_enabled,omitempty"`
	// Maximum aggregate total gas to be used for the contract executions in the
	// BeginBlocker.
	MaxBeginBlockTotalGas uint64 `protobuf:"varint,2,opt,name=max_begin_block_total_gas,json=maxBeginBlockTotalGas,proto3" json:"max_begin_block_total_gas,omitempty"`
	// the maximum gas limit each individual contract can consume in the
	// BeginBlocker.
	MaxContractGasLimit uint64 `protobuf:"varint,3,opt,name=max_contract_gas_limit,json=maxContractGasLimit,proto3" json:"max_contract_gas_limit,omitempty"`
	// min_gas_price defines the minimum gas price the contracts must pay to be
	// executed in the BeginBlocker.
	MinGasPrice uint64 `protobuf:"varint,4,opt,name=min_gas_price,json=minGasPrice,proto3" json:"min_gas_price,omitempty"`
}

func (m *Params) Reset()         { *m = Params{} }
func (m *Params) String() string { return proto.CompactTextString(m) }
func (*Params) ProtoMessage()    {}
func (*Params) Descriptor() ([]byte, []int) {
	return fileDescriptor_6818ff331f2cddc4, []int{0}
}
func (m *Params) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Params) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Params.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Params) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Params.Merge(m, src)
}
func (m *Params) XXX_Size() int {
	return m.Size()
}
func (m *Params) XXX_DiscardUnknown() {
	xxx_messageInfo_Params.DiscardUnknown(m)
}

var xxx_messageInfo_Params proto.InternalMessageInfo

func (m *Params) GetIsExecutionEnabled() bool {
	if m != nil {
		return m.IsExecutionEnabled
	}
	return false
}

func (m *Params) GetMaxBeginBlockTotalGas() uint64 {
	if m != nil {
		return m.MaxBeginBlockTotalGas
	}
	return 0
}

func (m *Params) GetMaxContractGasLimit() uint64 {
	if m != nil {
		return m.MaxContractGasLimit
	}
	return 0
}

func (m *Params) GetMinGasPrice() uint64 {
	if m != nil {
		return m.MinGasPrice
	}
	return 0
}

type RegisteredContract struct {
	// limit of gas per BB execution
	GasLimit uint64 `protobuf:"varint,1,opt,name=gas_limit,json=gasLimit,proto3" json:"gas_limit,omitempty"`
	// gas price that contract is willing to pay for execution in BeginBlocker
	GasPrice uint64 `protobuf:"varint,2,opt,name=gas_price,json=gasPrice,proto3" json:"gas_price,omitempty"`
	// is contract currently active
	IsExecutable bool `protobuf:"varint,3,opt,name=is_executable,json=isExecutable,proto3" json:"is_executable,omitempty"`
	// code_id that is allowed to be executed (to prevent malicious updates) - if
	// nil/0 any code_id can be executed
	CodeId uint64 `protobuf:"varint,4,opt,name=code_id,json=codeId,proto3" json:"code_id,omitempty"`
	// optional - admin addr that is allowed to update contract data
	AdminAddress string `protobuf:"bytes,5,opt,name=admin_address,json=adminAddress,proto3" json:"admin_address,omitempty"`
	// Optional: address of the contract granting fee
	// Must be set if fund_mode is GrantOnly
	GranterAddress string `protobuf:"bytes,6,opt,name=granter_address,json=granterAddress,proto3" json:"granter_address,omitempty"`
	// funding mode
	FundMode FundingMode `protobuf:"varint,7,opt,name=fund_mode,json=fundMode,proto3,enum=injective.wasmx.v1.FundingMode" json:"fund_mode,omitempty"`
}

func (m *RegisteredContract) Reset()         { *m = RegisteredContract{} }
func (m *RegisteredContract) String() string { return proto.CompactTextString(m) }
func (*RegisteredContract) ProtoMessage()    {}
func (*RegisteredContract) Descriptor() ([]byte, []int) {
	return fileDescriptor_6818ff331f2cddc4, []int{1}
}
func (m *RegisteredContract) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *RegisteredContract) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_RegisteredContract.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *RegisteredContract) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RegisteredContract.Merge(m, src)
}
func (m *RegisteredContract) XXX_Size() int {
	return m.Size()
}
func (m *RegisteredContract) XXX_DiscardUnknown() {
	xxx_messageInfo_RegisteredContract.DiscardUnknown(m)
}

var xxx_messageInfo_RegisteredContract proto.InternalMessageInfo

func (m *RegisteredContract) GetGasLimit() uint64 {
	if m != nil {
		return m.GasLimit
	}
	return 0
}

func (m *RegisteredContract) GetGasPrice() uint64 {
	if m != nil {
		return m.GasPrice
	}
	return 0
}

func (m *RegisteredContract) GetIsExecutable() bool {
	if m != nil {
		return m.IsExecutable
	}
	return false
}

func (m *RegisteredContract) GetCodeId() uint64 {
	if m != nil {
		return m.CodeId
	}
	return 0
}

func (m *RegisteredContract) GetAdminAddress() string {
	if m != nil {
		return m.AdminAddress
	}
	return ""
}

func (m *RegisteredContract) GetGranterAddress() string {
	if m != nil {
		return m.GranterAddress
	}
	return ""
}

func (m *RegisteredContract) GetFundMode() FundingMode {
	if m != nil {
		return m.FundMode
	}
	return FundingMode_Unspecified
}

func init() {
	proto.RegisterType((*Params)(nil), "injective.wasmx.v1.Params")
	proto.RegisterType((*RegisteredContract)(nil), "injective.wasmx.v1.RegisteredContract")
}

func init() { proto.RegisterFile("injective/wasmx/v1/wasmx.proto", fileDescriptor_6818ff331f2cddc4) }

var fileDescriptor_6818ff331f2cddc4 = []byte{
	// 486 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x92, 0x4f, 0x6e, 0x13, 0x31,
	0x14, 0xc6, 0xeb, 0x10, 0xd2, 0xc4, 0x34, 0x45, 0x32, 0x05, 0x0d, 0x45, 0x4c, 0x42, 0xd8, 0x84,
	0x45, 0x33, 0x94, 0x6e, 0x10, 0x62, 0x43, 0x50, 0x89, 0x2a, 0x8a, 0x54, 0x8d, 0x58, 0xb1, 0x19,
	0x79, 0xc6, 0x8f, 0xa9, 0x61, 0x6c, 0x47, 0xb6, 0x27, 0x0c, 0xb7, 0xe0, 0x08, 0x1c, 0x81, 0x63,
	0x74, 0x59, 0x89, 0x0d, 0x2b, 0x84, 0x92, 0x0d, 0xc7, 0x40, 0x9e, 0x7f, 0x54, 0x82, 0xdd, 0xf3,
	0xfb, 0x7d, 0xdf, 0x67, 0xfb, 0xe9, 0x61, 0x9f, 0xcb, 0x0f, 0x90, 0x58, 0xbe, 0x82, 0xe0, 0x13,
	0x35, 0xa2, 0x08, 0x56, 0x87, 0x55, 0x31, 0x5b, 0x6a, 0x65, 0x15, 0x21, 0x2d, 0x9f, 0x55, 0xed,
	0xd5, 0xe1, 0xfe, 0x5e, 0xaa, 0x52, 0x55, 0xe2, 0xc0, 0x55, 0x95, 0x72, 0xff, 0xc1, 0x7f, 0x92,
	0x96, 0x5a, 0x2d, 0x95, 0xa1, 0x59, 0x25, 0x99, 0x7c, 0x47, 0xb8, 0x77, 0x46, 0x35, 0x15, 0x86,
	0x3c, 0xc6, 0x7b, 0xdc, 0x44, 0x50, 0x40, 0x92, 0x5b, 0xae, 0x64, 0x04, 0x92, 0xc6, 0x19, 0x30,
	0x0f, 0x8d, 0xd1, 0xb4, 0x1f, 0x12, 0x6e, 0x8e, 0x1b, 0x74, 0x5c, 0x11, 0xf2, 0x14, 0xdf, 0x15,
	0xb4, 0x88, 0x62, 0x48, 0xb9, 0x8c, 0xe2, 0x4c, 0x25, 0x1f, 0x23, 0xab, 0x2c, 0xcd, 0xa2, 0x94,
	0x1a, 0xaf, 0x33, 0x46, 0xd3, 0x6e, 0x78, 0x5b, 0xd0, 0x62, 0xee, 0xf8, 0xdc, 0xe1, 0xb7, 0x8e,
	0x2e, 0xa8, 0x21, 0x47, 0xf8, 0x8e, 0x73, 0x26, 0x4a, 0x5a, 0x4d, 0x13, 0xeb, 0x0c, 0x51, 0xc6,
	0x05, 0xb7, 0xde, 0xb5, 0xd2, 0x76, 0x4b, 0xd0, 0xe2, 0x65, 0x0d, 0x17, 0xd4, 0x9c, 0x3a, 0x44,
	0x26, 0x78, 0x28, 0xb8, 0x2c, 0xb5, 0x4b, 0xcd, 0x13, 0xf0, 0xba, 0xa5, 0xf6, 0x86, 0xe0, 0x72,
	0x41, 0xcd, 0x99, 0x6b, 0x3d, 0xeb, 0xfe, 0xfe, 0x3a, 0x42, 0x93, 0x6f, 0x1d, 0x4c, 0x42, 0x48,
	0xb9, 0xb1, 0xa0, 0x81, 0x35, 0x41, 0xe4, 0x1e, 0x1e, 0xfc, 0xbd, 0x08, 0x95, 0xe6, 0x7e, 0xda,
	0xa4, 0xd7, 0xb0, 0x4a, 0xee, 0xb4, 0xb0, 0x8c, 0x25, 0x0f, 0xf1, 0xb0, 0x9d, 0x8d, 0xfb, 0x7b,
	0xf9, 0xcc, 0x7e, 0xb8, 0xd3, 0x0c, 0xc5, 0xf5, 0xc8, 0x7d, 0xbc, 0x9d, 0x28, 0x06, 0x11, 0x67,
	0xd5, 0xcb, 0xe6, 0xdd, 0x8b, 0x9f, 0x23, 0x14, 0xf6, 0x5c, 0xf3, 0x84, 0x91, 0x47, 0x78, 0x48,
	0x99, 0xfb, 0x00, 0x65, 0x4c, 0x83, 0x31, 0xde, 0xf5, 0x31, 0x9a, 0x0e, 0x6a, 0xd1, 0x4e, 0x89,
	0x5e, 0x54, 0x84, 0x1c, 0xe0, 0x9b, 0xa9, 0xa6, 0xd2, 0x82, 0x6e, 0xc5, 0xbd, 0x2b, 0xe2, 0xdd,
	0x1a, 0x36, 0xf2, 0xe7, 0x78, 0xf0, 0x3e, 0x97, 0x2c, 0x12, 0x8a, 0x81, 0xb7, 0x3d, 0x46, 0xd3,
	0xdd, 0x27, 0xa3, 0xd9, 0xbf, 0x5b, 0x32, 0x7b, 0x95, 0x4b, 0xc6, 0x65, 0xfa, 0x46, 0x31, 0x08,
	0xfb, 0xce, 0xe1, 0xaa, 0x6a, 0x64, 0x73, 0xb8, 0x58, 0xfb, 0xe8, 0x72, 0xed, 0xa3, 0x5f, 0x6b,
	0x1f, 0x7d, 0xd9, 0xf8, 0x5b, 0x97, 0x1b, 0x7f, 0xeb, 0xc7, 0xc6, 0xdf, 0x7a, 0xf7, 0x3a, 0xe5,
	0xf6, 0x3c, 0x8f, 0x67, 0x89, 0x12, 0xc1, 0x49, 0x13, 0x7a, 0x4a, 0x63, 0x13, 0xb4, 0x57, 0x1c,
	0x24, 0x4a, 0xc3, 0xd5, 0xe3, 0x39, 0xe5, 0x32, 0x10, 0x8a, 0xe5, 0x19, 0x98, 0x7a, 0xf7, 0xec,
	0xe7, 0x25, 0x98, 0xb8, 0x57, 0xae, 0xdd, 0xd1, 0x9f, 0x00, 0x00, 0x00, 0xff, 0xff, 0x2a, 0x6a,
	0x78, 0xe2, 0xe5, 0x02, 0x00, 0x00,
}

func (this *Params) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*Params)
	if !ok {
		that2, ok := that.(Params)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if this.IsExecutionEnabled != that1.IsExecutionEnabled {
		return false
	}
	if this.MaxBeginBlockTotalGas != that1.MaxBeginBlockTotalGas {
		return false
	}
	if this.MaxContractGasLimit != that1.MaxContractGasLimit {
		return false
	}
	if this.MinGasPrice != that1.MinGasPrice {
		return false
	}
	return true
}
func (this *RegisteredContract) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*RegisteredContract)
	if !ok {
		that2, ok := that.(RegisteredContract)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if this.GasLimit != that1.GasLimit {
		return false
	}
	if this.GasPrice != that1.GasPrice {
		return false
	}
	if this.IsExecutable != that1.IsExecutable {
		return false
	}
	if this.CodeId != that1.CodeId {
		return false
	}
	if this.AdminAddress != that1.AdminAddress {
		return false
	}
	if this.GranterAddress != that1.GranterAddress {
		return false
	}
	if this.FundMode != that1.FundMode {
		return false
	}
	return true
}
func (m *Params) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Params) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Params) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.MinGasPrice != 0 {
		i = encodeVarintWasmx(dAtA, i, uint64(m.MinGasPrice))
		i--
		dAtA[i] = 0x20
	}
	if m.MaxContractGasLimit != 0 {
		i = encodeVarintWasmx(dAtA, i, uint64(m.MaxContractGasLimit))
		i--
		dAtA[i] = 0x18
	}
	if m.MaxBeginBlockTotalGas != 0 {
		i = encodeVarintWasmx(dAtA, i, uint64(m.MaxBeginBlockTotalGas))
		i--
		dAtA[i] = 0x10
	}
	if m.IsExecutionEnabled {
		i--
		if m.IsExecutionEnabled {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *RegisteredContract) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RegisteredContract) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *RegisteredContract) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.FundMode != 0 {
		i = encodeVarintWasmx(dAtA, i, uint64(m.FundMode))
		i--
		dAtA[i] = 0x38
	}
	if len(m.GranterAddress) > 0 {
		i -= len(m.GranterAddress)
		copy(dAtA[i:], m.GranterAddress)
		i = encodeVarintWasmx(dAtA, i, uint64(len(m.GranterAddress)))
		i--
		dAtA[i] = 0x32
	}
	if len(m.AdminAddress) > 0 {
		i -= len(m.AdminAddress)
		copy(dAtA[i:], m.AdminAddress)
		i = encodeVarintWasmx(dAtA, i, uint64(len(m.AdminAddress)))
		i--
		dAtA[i] = 0x2a
	}
	if m.CodeId != 0 {
		i = encodeVarintWasmx(dAtA, i, uint64(m.CodeId))
		i--
		dAtA[i] = 0x20
	}
	if m.IsExecutable {
		i--
		if m.IsExecutable {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x18
	}
	if m.GasPrice != 0 {
		i = encodeVarintWasmx(dAtA, i, uint64(m.GasPrice))
		i--
		dAtA[i] = 0x10
	}
	if m.GasLimit != 0 {
		i = encodeVarintWasmx(dAtA, i, uint64(m.GasLimit))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func encodeVarintWasmx(dAtA []byte, offset int, v uint64) int {
	offset -= sovWasmx(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *Params) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.IsExecutionEnabled {
		n += 2
	}
	if m.MaxBeginBlockTotalGas != 0 {
		n += 1 + sovWasmx(uint64(m.MaxBeginBlockTotalGas))
	}
	if m.MaxContractGasLimit != 0 {
		n += 1 + sovWasmx(uint64(m.MaxContractGasLimit))
	}
	if m.MinGasPrice != 0 {
		n += 1 + sovWasmx(uint64(m.MinGasPrice))
	}
	return n
}

func (m *RegisteredContract) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.GasLimit != 0 {
		n += 1 + sovWasmx(uint64(m.GasLimit))
	}
	if m.GasPrice != 0 {
		n += 1 + sovWasmx(uint64(m.GasPrice))
	}
	if m.IsExecutable {
		n += 2
	}
	if m.CodeId != 0 {
		n += 1 + sovWasmx(uint64(m.CodeId))
	}
	l = len(m.AdminAddress)
	if l > 0 {
		n += 1 + l + sovWasmx(uint64(l))
	}
	l = len(m.GranterAddress)
	if l > 0 {
		n += 1 + l + sovWasmx(uint64(l))
	}
	if m.FundMode != 0 {
		n += 1 + sovWasmx(uint64(m.FundMode))
	}
	return n
}

func sovWasmx(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozWasmx(x uint64) (n int) {
	return sovWasmx(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Params) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowWasmx
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
			return fmt.Errorf("proto: Params: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Params: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field IsExecutionEnabled", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWasmx
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
			m.IsExecutionEnabled = bool(v != 0)
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MaxBeginBlockTotalGas", wireType)
			}
			m.MaxBeginBlockTotalGas = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWasmx
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MaxBeginBlockTotalGas |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MaxContractGasLimit", wireType)
			}
			m.MaxContractGasLimit = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWasmx
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MaxContractGasLimit |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MinGasPrice", wireType)
			}
			m.MinGasPrice = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWasmx
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MinGasPrice |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipWasmx(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthWasmx
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
func (m *RegisteredContract) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowWasmx
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
			return fmt.Errorf("proto: RegisteredContract: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RegisteredContract: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field GasLimit", wireType)
			}
			m.GasLimit = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWasmx
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.GasLimit |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field GasPrice", wireType)
			}
			m.GasPrice = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWasmx
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.GasPrice |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field IsExecutable", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWasmx
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
			m.IsExecutable = bool(v != 0)
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field CodeId", wireType)
			}
			m.CodeId = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWasmx
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.CodeId |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field AdminAddress", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWasmx
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
				return ErrInvalidLengthWasmx
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthWasmx
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.AdminAddress = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 6:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field GranterAddress", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWasmx
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
				return ErrInvalidLengthWasmx
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthWasmx
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.GranterAddress = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 7:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field FundMode", wireType)
			}
			m.FundMode = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowWasmx
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.FundMode |= FundingMode(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipWasmx(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthWasmx
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
func skipWasmx(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowWasmx
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
					return 0, ErrIntOverflowWasmx
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
					return 0, ErrIntOverflowWasmx
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
				return 0, ErrInvalidLengthWasmx
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupWasmx
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthWasmx
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthWasmx        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowWasmx          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupWasmx = fmt.Errorf("proto: unexpected end of group")
)
