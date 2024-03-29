// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.6.1
// source: rowdata.proto

package kvrows

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type TransactionState int32

const (
	TransactionState_Active    TransactionState = 0
	TransactionState_Committed TransactionState = 1
	TransactionState_Aborted   TransactionState = 2
)

// Enum value maps for TransactionState.
var (
	TransactionState_name = map[int32]string{
		0: "Active",
		1: "Committed",
		2: "Aborted",
	}
	TransactionState_value = map[string]int32{
		"Active":    0,
		"Committed": 1,
		"Aborted":   2,
	}
)

func (x TransactionState) Enum() *TransactionState {
	p := new(TransactionState)
	*p = x
	return p
}

func (x TransactionState) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (TransactionState) Descriptor() protoreflect.EnumDescriptor {
	return file_rowdata_proto_enumTypes[0].Descriptor()
}

func (TransactionState) Type() protoreflect.EnumType {
	return &file_rowdata_proto_enumTypes[0]
}

func (x TransactionState) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use TransactionState.Descriptor instead.
func (TransactionState) EnumDescriptor() ([]byte, []int) {
	return file_rowdata_proto_rawDescGZIP(), []int{0}
}

type RowData struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Proposal *ProposalData `protobuf:"bytes,1,opt,name=Proposal,proto3" json:"Proposal,omitempty"`
	Rows     []*RowValue   `protobuf:"bytes,2,rep,name=Rows,proto3" json:"Rows,omitempty"`
}

func (x *RowData) Reset() {
	*x = RowData{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rowdata_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RowData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RowData) ProtoMessage() {}

func (x *RowData) ProtoReflect() protoreflect.Message {
	mi := &file_rowdata_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RowData.ProtoReflect.Descriptor instead.
func (*RowData) Descriptor() ([]byte, []int) {
	return file_rowdata_proto_rawDescGZIP(), []int{0}
}

func (x *RowData) GetProposal() *ProposalData {
	if x != nil {
		return x.Proposal
	}
	return nil
}

func (x *RowData) GetRows() []*RowValue {
	if x != nil {
		return x.Rows
	}
	return nil
}

type RowValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Version uint64 `protobuf:"varint,1,opt,name=Version,proto3" json:"Version,omitempty"`
	Value   []byte `protobuf:"bytes,2,opt,name=Value,proto3" json:"Value,omitempty"`
}

func (x *RowValue) Reset() {
	*x = RowValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rowdata_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RowValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RowValue) ProtoMessage() {}

func (x *RowValue) ProtoReflect() protoreflect.Message {
	mi := &file_rowdata_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RowValue.ProtoReflect.Descriptor instead.
func (*RowValue) Descriptor() ([]byte, []int) {
	return file_rowdata_proto_rawDescGZIP(), []int{1}
}

func (x *RowValue) GetVersion() uint64 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *RowValue) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type ProposalData struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TXID    uint64            `protobuf:"varint,1,opt,name=TXID,proto3" json:"TXID,omitempty"`
	Updates []*ProposedUpdate `protobuf:"bytes,2,rep,name=Updates,proto3" json:"Updates,omitempty"`
}

func (x *ProposalData) Reset() {
	*x = ProposalData{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rowdata_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProposalData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProposalData) ProtoMessage() {}

func (x *ProposalData) ProtoReflect() protoreflect.Message {
	mi := &file_rowdata_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProposalData.ProtoReflect.Descriptor instead.
func (*ProposalData) Descriptor() ([]byte, []int) {
	return file_rowdata_proto_rawDescGZIP(), []int{2}
}

func (x *ProposalData) GetTXID() uint64 {
	if x != nil {
		return x.TXID
	}
	return 0
}

func (x *ProposalData) GetUpdates() []*ProposedUpdate {
	if x != nil {
		return x.Updates
	}
	return nil
}

type ProposedUpdate struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	SID   uint32 `protobuf:"varint,1,opt,name=SID,proto3" json:"SID,omitempty"` // Statement ID
	Value []byte `protobuf:"bytes,2,opt,name=Value,proto3" json:"Value,omitempty"`
}

func (x *ProposedUpdate) Reset() {
	*x = ProposedUpdate{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rowdata_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProposedUpdate) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProposedUpdate) ProtoMessage() {}

func (x *ProposedUpdate) ProtoReflect() protoreflect.Message {
	mi := &file_rowdata_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProposedUpdate.ProtoReflect.Descriptor instead.
func (*ProposedUpdate) Descriptor() ([]byte, []int) {
	return file_rowdata_proto_rawDescGZIP(), []int{3}
}

func (x *ProposedUpdate) GetSID() uint32 {
	if x != nil {
		return x.SID
	}
	return 0
}

func (x *ProposedUpdate) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type TransactionData struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Epoch       uint64           `protobuf:"varint,1,opt,name=Epoch,proto3" json:"Epoch,omitempty"`
	State       TransactionState `protobuf:"varint,2,opt,name=State,proto3,enum=TransactionState" json:"State,omitempty"`
	Version     uint64           `protobuf:"varint,3,opt,name=Version,proto3" json:"Version,omitempty"`
	UpdatedKeys [][]byte         `protobuf:"bytes,4,rep,name=UpdatedKeys,proto3" json:"UpdatedKeys,omitempty"`
}

func (x *TransactionData) Reset() {
	*x = TransactionData{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rowdata_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TransactionData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TransactionData) ProtoMessage() {}

func (x *TransactionData) ProtoReflect() protoreflect.Message {
	mi := &file_rowdata_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TransactionData.ProtoReflect.Descriptor instead.
func (*TransactionData) Descriptor() ([]byte, []int) {
	return file_rowdata_proto_rawDescGZIP(), []int{4}
}

func (x *TransactionData) GetEpoch() uint64 {
	if x != nil {
		return x.Epoch
	}
	return 0
}

func (x *TransactionData) GetState() TransactionState {
	if x != nil {
		return x.State
	}
	return TransactionState_Active
}

func (x *TransactionData) GetVersion() uint64 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *TransactionData) GetUpdatedKeys() [][]byte {
	if x != nil {
		return x.UpdatedKeys
	}
	return nil
}

var File_rowdata_proto protoreflect.FileDescriptor

var file_rowdata_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x72, 0x6f, 0x77, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22,
	0x53, 0x0a, 0x07, 0x52, 0x6f, 0x77, 0x44, 0x61, 0x74, 0x61, 0x12, 0x29, 0x0a, 0x08, 0x50, 0x72,
	0x6f, 0x70, 0x6f, 0x73, 0x61, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x50,
	0x72, 0x6f, 0x70, 0x6f, 0x73, 0x61, 0x6c, 0x44, 0x61, 0x74, 0x61, 0x52, 0x08, 0x50, 0x72, 0x6f,
	0x70, 0x6f, 0x73, 0x61, 0x6c, 0x12, 0x1d, 0x0a, 0x04, 0x52, 0x6f, 0x77, 0x73, 0x18, 0x02, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x09, 0x2e, 0x52, 0x6f, 0x77, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x04,
	0x52, 0x6f, 0x77, 0x73, 0x22, 0x3a, 0x0a, 0x08, 0x52, 0x6f, 0x77, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x12, 0x18, 0x0a, 0x07, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x04, 0x52, 0x07, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x14, 0x0a, 0x05, 0x56, 0x61,
	0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x22, 0x4d, 0x0a, 0x0c, 0x50, 0x72, 0x6f, 0x70, 0x6f, 0x73, 0x61, 0x6c, 0x44, 0x61, 0x74, 0x61,
	0x12, 0x12, 0x0a, 0x04, 0x54, 0x58, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x04,
	0x54, 0x58, 0x49, 0x44, 0x12, 0x29, 0x0a, 0x07, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x73, 0x18,
	0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x50, 0x72, 0x6f, 0x70, 0x6f, 0x73, 0x65, 0x64,
	0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x52, 0x07, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x73, 0x22,
	0x38, 0x0a, 0x0e, 0x50, 0x72, 0x6f, 0x70, 0x6f, 0x73, 0x65, 0x64, 0x55, 0x70, 0x64, 0x61, 0x74,
	0x65, 0x12, 0x10, 0x0a, 0x03, 0x53, 0x49, 0x44, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x03,
	0x53, 0x49, 0x44, 0x12, 0x14, 0x0a, 0x05, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x05, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x8c, 0x01, 0x0a, 0x0f, 0x54, 0x72,
	0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x44, 0x61, 0x74, 0x61, 0x12, 0x14, 0x0a,
	0x05, 0x45, 0x70, 0x6f, 0x63, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x05, 0x45, 0x70,
	0x6f, 0x63, 0x68, 0x12, 0x27, 0x0a, 0x05, 0x53, 0x74, 0x61, 0x74, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0e, 0x32, 0x11, 0x2e, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x05, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x18, 0x0a, 0x07,
	0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x56,
	0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x20, 0x0a, 0x0b, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65,
	0x64, 0x4b, 0x65, 0x79, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0c, 0x52, 0x0b, 0x55, 0x70, 0x64,
	0x61, 0x74, 0x65, 0x64, 0x4b, 0x65, 0x79, 0x73, 0x2a, 0x3a, 0x0a, 0x10, 0x54, 0x72, 0x61, 0x6e,
	0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x0a, 0x0a, 0x06,
	0x41, 0x63, 0x74, 0x69, 0x76, 0x65, 0x10, 0x00, 0x12, 0x0d, 0x0a, 0x09, 0x43, 0x6f, 0x6d, 0x6d,
	0x69, 0x74, 0x74, 0x65, 0x64, 0x10, 0x01, 0x12, 0x0b, 0x0a, 0x07, 0x41, 0x62, 0x6f, 0x72, 0x74,
	0x65, 0x64, 0x10, 0x02, 0x42, 0x0a, 0x5a, 0x08, 0x2e, 0x2f, 0x6b, 0x76, 0x72, 0x6f, 0x77, 0x73,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_rowdata_proto_rawDescOnce sync.Once
	file_rowdata_proto_rawDescData = file_rowdata_proto_rawDesc
)

func file_rowdata_proto_rawDescGZIP() []byte {
	file_rowdata_proto_rawDescOnce.Do(func() {
		file_rowdata_proto_rawDescData = protoimpl.X.CompressGZIP(file_rowdata_proto_rawDescData)
	})
	return file_rowdata_proto_rawDescData
}

var file_rowdata_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_rowdata_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_rowdata_proto_goTypes = []interface{}{
	(TransactionState)(0),   // 0: TransactionState
	(*RowData)(nil),         // 1: RowData
	(*RowValue)(nil),        // 2: RowValue
	(*ProposalData)(nil),    // 3: ProposalData
	(*ProposedUpdate)(nil),  // 4: ProposedUpdate
	(*TransactionData)(nil), // 5: TransactionData
}
var file_rowdata_proto_depIdxs = []int32{
	3, // 0: RowData.Proposal:type_name -> ProposalData
	2, // 1: RowData.Rows:type_name -> RowValue
	4, // 2: ProposalData.Updates:type_name -> ProposedUpdate
	0, // 3: TransactionData.State:type_name -> TransactionState
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_rowdata_proto_init() }
func file_rowdata_proto_init() {
	if File_rowdata_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_rowdata_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RowData); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rowdata_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RowValue); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rowdata_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProposalData); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rowdata_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProposedUpdate); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rowdata_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TransactionData); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_rowdata_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_rowdata_proto_goTypes,
		DependencyIndexes: file_rowdata_proto_depIdxs,
		EnumInfos:         file_rowdata_proto_enumTypes,
		MessageInfos:      file_rowdata_proto_msgTypes,
	}.Build()
	File_rowdata_proto = out.File
	file_rowdata_proto_rawDesc = nil
	file_rowdata_proto_goTypes = nil
	file_rowdata_proto_depIdxs = nil
}
