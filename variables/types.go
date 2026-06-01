package ValkeyAdapter

// DataType is the canonical set of variable data types recognized across MDAI
// components (operator CRD, gateway, event-hub, data-core). The literal string
// values match the kubebuilder enum on MdaiHub's VariableDataType field and are
// the wire format used in event payloads and audit entries.
type DataType string

const (
	DataTypeInt     DataType = "int"     // signed integer, stored as a string in Valkey
	DataTypeFloat   DataType = "float"   // 64-bit float, stored as a string in Valkey; writers must canonicalize via strconv.FormatFloat(v, 'g', -1, 64)
	DataTypeBoolean DataType = "boolean" // "true" or "false" as a string in Valkey
	DataTypeString  DataType = "string"
	DataTypeSet     DataType = "set" // Valkey SET
	DataTypeMap     DataType = "map" // Valkey HASH; field-value pairs are strings

	DataTypeMetaHashSet      DataType = "metaHashSet"      // module-backed lookup table (input key → value via a referenced set)
	DataTypeMetaPriorityList DataType = "metaPriorityList" // module-backed ordered references; evaluates to the first non-empty
)
