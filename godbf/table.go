package godbf

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/axgle/mahonia"
)

const (
	yearOffset      = 1900
	null       byte = 0x00
	blank      byte = 0x20

	fieldNameByteLength          = 11
	maxUsableNameByteLength      = fieldNameByteLength - 1
	endOfFieldNameMarker    byte = 0x0

	recordDeletionFlagIndex = 0
	recordIsActive          = blank
	recordIsDeleted         = 0x2A

	eofMarker byte = 0x1A
)

// DbfTable is an in-memory container for dbase formatted data, and state that helps manage that data.
type DbfTable struct {
	dbaseData

	tableManagement
	imageCache
}

// dbaseData stores dBase file information in byte-arrays, for convnient saving and loading dBase compatible files.
// For reference: https://en.wikipedia.org/wiki/.dbf#File_format_of_Level_5_DOS_dBASE
type dbaseData struct {
	header
	records
	eofMarker byte
}

// dbase file header information
type header struct {
	fileSignature uint8 // Valid dBASE III PLUS table file (03h without a memo .DBT file; 83h with a memo)
	dateOfLastUpdate
	numberOfRecords       uint32   // Number of records in the table.
	numberOfBytesInHeader uint16   // Number of bytes in the header.
	lengthOfEachRecord    uint16   // Number of bytes in the record.
	fieldDescriptor       [32]byte // Field descriptor array

	// columns of dbase file
	fields          []FieldDescriptor
	fieldTerminator int8 // 0Dh stored as the field terminator.
}

// SetNumberOfRecordsFromBytes sets numberOfRecords from a byte array.
func (h *header) SetNumberOfRecordsFromBytes(s []byte) {
	h.numberOfRecords = uint32(s[0]) | (uint32(s[1]) << 8) | (uint32(s[2]) << 16) | (uint32(s[3]) << 24)
}

// SetNumberOfBytesInHeaderFromBytes sets numberOfBytesInHeader from a byte array.
func (h *header) SetNumberOfBytesInHeaderFromBytes(s []byte) {
	h.numberOfBytesInHeader = uint16(s[0]) | (uint16(s[1]) << 8)
}

// SetLengthOfEachRecordFromBytes sets lengthOfEachRecord from a byte array.
func (h *header) SetLengthOfEachRecordFromBytes(s []byte) {
	h.lengthOfEachRecord = uint16(s[0]) | (uint16(s[1]) << 8)
}

// dateOfLastUpdate holds the date of last update; in YYMMDD format where each is stored in a single byte, as per dBase.
// A consequence of this is that the lowest level of granularity supported is a whole 24-hour day.
// Because measures smaller than a day cannot be encoded, for any time.Time conversion, the 'time of day' for a given
// encoded day is assumed to be 12:00:00AM of that day.
//
// Timezones are also not supported. All date manipulation done via this struct assume the time.Local location applies.
// Callers should thus be careful to ensure that they are using time.Local as their location  when interfacing
// with the various decorator methods.
//
// The updateYear byte encodes the year number (0-255). The actual gregorian calendar year is derived by adding
// 1900 to the byte's value. Consequently, the range of years supported is [1900-2155] inclusive.
//
// The updateMonth byte encodes the 0-indexed month number (0-11).
//
// The updateDay byte encodes the 0-indexed day of the month (0-30).
type dateOfLastUpdate struct {
	updateYear  uint8 // YY + yearOffset (1900) = actual year.
	updateMonth uint8
	updateDay   uint8
}

// RefreshLastUpdated refreshes the dateOfLastUpdate to the YYMMDD byte encoding of today, assuming the local timezone.
// SetLastUpdated() is used by this method, and the same restrictions for it apply here.
func (ud *dateOfLastUpdate) RefreshLastUpdated() {
	ud.SetLastUpdated(time.Now())
}

// SetLastUpdated sets the dateOfLastUpdate to the YYMMDD byte encoding of the time.Time specified.
// See dateOfLastUpdate for the various limitations present in interpreting time.Time.
func (ud *dateOfLastUpdate) SetLastUpdated(updateTime time.Time) {
	ud.updateYear = byte(updateTime.Year() - yearOffset)
	ud.updateMonth = byte(updateTime.Month())
	ud.updateDay = byte(updateTime.Day())
}

// SetLastUpdatedFromBytes sets the dateOfLastUpdate to the YYMMDD byte encoding of the time specified.
// The 0-index is assigned to updateYear, the 1-index byte to updateMonth, and the 2-index byte to updateDay.
//
// See dateOfLastUpdate for further detail on appropriate byte values.
func (ud *dateOfLastUpdate) SetLastUpdatedFromBytes(timeBytes []byte) {
	ud.updateYear = timeBytes[0]
	ud.updateMonth = timeBytes[1]
	ud.updateDay = timeBytes[2]
}

// LastUpdated interprets the byte trio in dateOfLastUpdate, returning as close a time.Time value as possible.
// As no hours, minutes, seconds, etc.  are supported in the encoding, we assume 12:00:00AM for the return time.
// Similarly, time.Local is assumed for the location.
func (ud *dateOfLastUpdate) LastUpdated() time.Time {
	updateTime := time.Date(
		int(ud.updateYear)+yearOffset,
		time.Month(ud.updateMonth),
		int(ud.updateDay),
		0, 0, 0, 0,
		time.Local)

	return updateTime
}

// LowDefTime takes a time.Time and returns a low-definition time.Time equivalent that follows the same simplification
// approach as LastUpdated().
func (ud *dateOfLastUpdate) LowDefTime(highDefTime time.Time) time.Time {
	lowDefTime := time.Date(
		highDefTime.Year(),
		highDefTime.Month(),
		highDefTime.Day(),
		0, 0, 0, 0,
		time.Local)
	return lowDefTime
}

type records []record

type record struct {
	deletionFlag byte
	recordValue  string
}

// tableManagement is an aggregate of structs and their methods that are not part of the dBase file
// standard, but nevertheless useful in managing an in-memory DbfTable.
type tableManagement struct {
	numberOfFields int            // number of fields/columns in dbase file
	fieldMap       map[string]int // used to map field names to index

	schemaLockable
	createdFromScratch bool // used before adding new fields to increment nu
	encodingSupport
}

// schemaLockable permits or denys updates to the database field-definitions. You can only add new records when the
// schema has become locked.
type schemaLockable struct {
	schemaLocked bool
}

// encoding provides text encoding support for DbfTable
type encodingSupport struct {
	textEncoding string
	decoder      mahonia.Decoder
	encoder      mahonia.Encoder
}

func (es *encodingSupport) UseEncoding(encoding string) {
	es.textEncoding = encoding
	es.encoder = mahonia.NewEncoder(encoding)
	es.decoder = mahonia.NewDecoder(encoding)
}

func (es *encodingSupport) GetEncoding() string {
	return es.textEncoding
}

// imageCache keeps a dbase table in memory as its byte array encoding
type imageCache struct {
	dataStore []byte
}

// AddEOFMarker add eof to the end of table file
func (dt *DbfTable) AddEOFMarker() {
	dt.dataStore = append(dt.dataStore, dt.eofMarker)
}

// AddFieldAs add field to dst like another from src
func (dt *DbfTable) AddFieldAs(src *FieldDescriptor, name string) (err error) {
	switch src.FieldType() {
	case Character:
		err = dt.AddTextField(name, src.Length())
	case Date:
		err = dt.AddDateField(name)
	case Float:
		err = dt.AddFloatField(name, src.Length(), src.DecimalPlaces())
	case Logical:
		err = dt.AddBooleanField(name)
	case Numeric:
		err = dt.AddNumberField(name, src.Length(), src.DecimalPlaces())
	}
	if err != nil {
		return err
	}
	return nil
}

func (dt *DbfTable) AddBooleanField(fieldName string) (err error) {
	return dt.addField(fieldName, Logical, Logical.fixedFieldLength(), Logical.decimalCountNotApplicable())
}

func (dt *DbfTable) AddDateField(fieldName string) (err error) {
	return dt.addField(fieldName, Date, Date.fixedFieldLength(), Date.decimalCountNotApplicable())
}

func (dt *DbfTable) AddTextField(fieldName string, length byte) (err error) {
	return dt.addField(fieldName, Character, length, Character.decimalCountNotApplicable())
}

func (dt *DbfTable) AddNumberField(fieldName string, length byte, decimalPlaces uint8) (err error) {
	return dt.addField(fieldName, Numeric, length, decimalPlaces)
}

func (dt *DbfTable) AddFloatField(fieldName string, length byte, decimalPlaces uint8) (err error) {
	return dt.addField(fieldName, Float, length, decimalPlaces)
}

func (dt *DbfTable) CreateField(fieldName string, fieldType DbaseDataType, length byte, decimalPlaces uint8) FieldDescriptor {
	df := new(FieldDescriptor)
	df.name = fieldName
	df.fieldType = fieldType
	df.length = length
	df.decimalPlaces = decimalPlaces

	slice := dt.convertToByteSlice(df.name, fieldNameByteLength)
	// Field name in ASCII (max 10 chracters)
	// for i := 0; i < len(slice); i++ {
	// 	df.fieldStore[i] = slice[i]
	// }
	copy(df.fieldStore[:], slice)

	df.fieldStore[fieldNameByteLength] = endOfFieldNameMarker
	// Set field's data type
	// C (Character)  All OEM code page characters.
	// D (Date)     Numbers and a character to separate month, day, and year (stored internally as 8 digits in YYYYMMDD format).
	// N (Numeric)    - . 0 1 2 3 4 5 6 7 8 9
	// F (Floating Point)   - . 0 1 2 3 4 5 6 7 8 9
	// L (Logical)    ? Y y N n T t F f (? when not initialized).
	df.fieldStore[11] = df.fieldType.byte()

	// fixedFieldLength of field
	df.fieldStore[16] = df.length

	// number of decimal places
	// Applicable only to number/float
	df.fieldStore[17] = df.decimalPlaces
	return *df
}

func (dt *DbfTable) addField(fieldName string, fieldType DbaseDataType, length byte, decimalPlaces uint8) (err error) {
	if dt.schemaLocked {
		return errors.New("once you start entering data to the dbase table or open an existing dbase file, altering dbase table schema is not allowed")
	}

	normalizedFieldName := dt.normaliseFieldName(fieldName)

	if dt.HasField(normalizedFieldName) {
		return errors.New("Field name \"" + normalizedFieldName + "\" already exists")
	}

	df := dt.CreateField(normalizedFieldName, fieldType, length, decimalPlaces)
	dt.fields = append(dt.fields, df)

	// if createdFromScratch we need to update dbase header to reflect the changes we have made
	if dt.createdFromScratch {
		dt.UpdateHeader()
	}

	return
}

// AddNewField add an new field to a exist dbf file
func (dt *DbfTable) AddNewField(fieldName string, fieldType DbaseDataType, length byte, decimalPlaces uint8) (err error) {
	if dt.schemaLocked {
		return errors.New("once you start entering data to the dbase table or open an existing dbase file, altering dbase table schema is not allowed")
	}
	normalizedFieldName := dt.normaliseFieldName(fieldName)
	if dt.HasField(normalizedFieldName) {
		return errors.New("new field name \"" + normalizedFieldName + "\" already exists")
	}

	err = dt.addField(fieldName, fieldType, length, decimalPlaces)
	if err != nil {
		return
	}

	dt.UpdateHeader()

	// no records as yet
	dt.AddEOFMarker()
	return
}

func (dt *DbfTable) AlterField(fieldName string, fieldNewName string, fieldType DbaseDataType, length byte, decimalPlaces uint8) (err error) {
	if dt.schemaLocked {
		return errors.New("once you start entering data to the dbase table or open an existing dbase file, altering dbase table schema is not allowed")
	}

	if fieldNewName == "" {
		fieldNewName = fieldName
	}
	normalizedFieldName := dt.normaliseFieldName(fieldName)
	normalizedFieldNewName := dt.normaliseFieldName(fieldNewName)

	if !dt.HasField(normalizedFieldName) {
		return errors.New("field name \"" + normalizedFieldName + "\" is not exist")
	}

	if normalizedFieldNewName != normalizedFieldName && dt.HasField(normalizedFieldNewName) {
		return errors.New("new field name \"" + normalizedFieldNewName + "\" already exists")
	}

	idx, err := dt.FieldIndex(fieldName)
	if err != nil {
		return
	}

	if length == byte(0) {
		length = dt.fields[idx].length
	}
	if fieldType == DbaseDataType(0) {
		fieldType = dt.fields[idx].fieldType
	}
	if decimalPlaces == byte(0) {
		decimalPlaces = dt.fields[idx].decimalPlaces
	}

	dt.fields[idx] = dt.CreateField(normalizedFieldNewName, fieldType, length, decimalPlaces)
	dt.UpdateHeader()

	// no records as yet
	dt.AddEOFMarker()
	return
}
func (dt *DbfTable) DeleteField(fieldName string) (err error) {
	if dt.schemaLocked {
		return errors.New("once you start entering data to the dbase table or open an existing dbase file, altering dbase table schema is not allowed")
	}

	normalizedFieldName := dt.normaliseFieldName(fieldName)

	if !dt.HasField(normalizedFieldName) {
		return errors.New("Field name \"" + normalizedFieldName + "\" is not exist")
	}

	idx, err := dt.FieldIndex(fieldName)

	if err != nil {
		return
	}

	dt.fields = append(dt.fields[:idx], dt.fields[idx+1:]...)
	dt.UpdateHeader()

	// no records as yet
	dt.AddEOFMarker()
	return
}

func (dt *DbfTable) normaliseFieldName(name string) (s string) {
	e := mahonia.NewEncoder(dt.textEncoding)
	b := []byte(e.ConvertString(name))

	if len(b) > maxUsableNameByteLength {
		b = b[0:maxUsableNameByteLength]
	}

	d := mahonia.NewDecoder(dt.textEncoding)
	s = d.ConvertString(string(b))

	return
}

/*
getByteSlice converts value to byte slice according to given encoding and return
a slice that is fixedFieldLength equals to numberOfBytes or less if the string is shorter than
numberOfBytes
*/
func (dt *DbfTable) convertToByteSlice(value string, numberOfBytes int) (s []byte) {
	e := mahonia.NewEncoder(dt.textEncoding)
	b := []byte(e.ConvertString(value))

	if len(b) <= numberOfBytes {
		s = b
	} else {
		s = b[0:numberOfBytes]
	}
	return
}

func (dt *DbfTable) UpdateHeader() {
	// first create a slice from initial 32 bytes of datastore as the foundation of the new slice
	// later we will set this slice to dt.dataStore to create the new header slice
	slice := dt.dataStore[0:32]

	// set dbase file signature
	slice[0] = 0x03

	var lengthOfEachRecord uint16 = 0

	for i := range dt.Fields() {
		lengthOfEachRecord += uint16(dt.Fields()[i].length)
		slice = append(slice, dt.Fields()[i].fieldStore[:]...)

		// don't forget to update fieldMap. We need it to find the index of a field name
		dt.fieldMap[dt.Fields()[i].name] = i
	}

	// end of file header terminator (0Dh)
	slice = append(slice, 0x0D)

	// now reset dt.dataStore slice with the updated one
	dt.dataStore = slice

	// update number of records to 0
	dt.numberOfRecords = uint32(0)
	s := uint32ToBytes(dt.numberOfRecords)
	dt.dataStore[4] = s[0]
	dt.dataStore[5] = s[1]
	dt.dataStore[6] = s[2]
	dt.dataStore[7] = s[3]

	// update the number of bytes in dbase file header
	dt.numberOfBytesInHeader = uint16(len(slice))
	s = uint32ToBytes(uint32(dt.numberOfBytesInHeader))
	dt.dataStore[8] = s[0]
	dt.dataStore[9] = s[1]

	dt.lengthOfEachRecord = lengthOfEachRecord + 1 // don't forget to add "1" for deletion marker which is 20h

	// update the length of each record
	s = uint32ToBytes(uint32(dt.lengthOfEachRecord))
	dt.dataStore[10] = s[0]
	dt.dataStore[11] = s[1]

	return
}

// Fields return the fields of the table as a slice
func (dt *DbfTable) Fields() []FieldDescriptor {
	return dt.fields
}

// FieldByName return the field descriptor of the table
func (dt *DbfTable) FieldByName(fieldName string) FieldDescriptor {

	for i := 0; i < len(dt.fields); i++ {
		if dt.fields[i].name == fieldName {
			return dt.fields[i]
		}
	}

	return FieldDescriptor{}
}

// FieldNames return the names of fields in the table as a slice
func (dt *DbfTable) FieldNames() []string {
	names := make([]string, 0)

	for _, field := range dt.Fields() {
		names = append(names, field.name)
	}

	return names
}

// FieldIdx returns field index if the table has a field with the given name
// If the field does not exist an error is returned.
func (dt *DbfTable) FieldIndex(fieldName string) (int, error) {

	for i := 0; i < len(dt.fields); i++ {
		if dt.fields[i].name == fieldName {
			return i, nil
		}
	}
	return -1, errors.New("Field name \"" + fieldName + "\" not exists!")
}

// HasField returns true if the table has a field with the given name
// If the field does not exist an error is returned.
func (dt *DbfTable) HasField(fieldName string) bool {

	for i := 0; i < len(dt.fields); i++ {
		if dt.fields[i].name == fieldName {
			return true
		}
	}

	return false
}

// DecimalPlacesInField returns the number of decimal places for the field with the given name.
// If the field does not exist, or does not use decimal places, an error is returned.
func (dt *DbfTable) DecimalPlacesInField(fieldName string) (uint8, error) {
	if !dt.HasField(fieldName) {
		return 0, errors.New("Field name \"" + fieldName + "\" does not exist. ")
	}

	for i := 0; i < len(dt.fields); i++ {
		if dt.fields[i].name == fieldName && dt.fields[i].usesDecimalPlaces() {
			return dt.fields[i].decimalPlaces, nil
		}
	}

	return 0, errors.New("type of field \"" + fieldName + "\" is not Numeric or Float")
}

// AddNewRecord adds a new empty record to the table, and returns the index number of the record.
func (dt *DbfTable) AddNewRecord() (newRecordNumber int, addErr error) {
	if dt.lengthOfEachRecord <= 1 {
		return -1, errors.New("attempted to add record with no fields defined")
	}
	lockSchema(dt)

	newRecord := make([]byte, dt.lengthOfEachRecord)
	newRecord[recordDeletionFlagIndex] = recordIsActive
	dt.dataStore = append(dt.dataStore, newRecord...)

	// since row numbers are "0" based first we set newRecordNumber
	// and then increment number of records in dbase table
	newRecordNumber = int(dt.numberOfRecords)

	//fmt.Printf("Number of rows before:%d\n", dt.numberOfRecords)
	dt.numberOfRecords++
	s := uint32ToBytes(dt.numberOfRecords)
	dt.dataStore[4] = s[0]
	dt.dataStore[5] = s[1]
	dt.dataStore[6] = s[2]
	dt.dataStore[7] = s[3]
	//fmt.Printf("Number of rows after:%d\n", dt.numberOfRecords)

	return newRecordNumber, nil
}

// NumberOfRecords returns the number of records in the table
func (dt *DbfTable) NumberOfRecords() (ret int) {
	defer func() {
		if r := recover(); r != nil {
			ret = 0
		}
	}()
	return int(dt.numberOfRecords)
}

// HasRecord returns true if the table has a record with the given number otherwise, false is returned.
// Use this method before FieldValue() to avoid index-out-of-range errors.
func (dt *DbfTable) HasRecord(recordNumber int) bool {
	recordOffset := int(dt.numberOfBytesInHeader) + recordNumber*int(dt.lengthOfEachRecord)
	return len(dt.dataStore) >= recordOffset+int(dt.lengthOfEachRecord)
}

// SetFieldValueByName sets the value for the given row and field name as specified
// If the field name does not exist, or the value is incompatible with the field's type, an error is returned.
func (dt *DbfTable) SetFieldValueByName(row int, fieldName string, value string) (err error) {
	if fieldIndex, found := dt.fieldMap[fieldName]; found {
		return dt.SetFieldValue(row, fieldIndex, value)
	}
	return errors.New("Field name \"" + fieldName + "\" does not exist")
}

// SetFieldValue sets the value for the given row and field index as specified
// If the field index is invalid, or the value is incompatible with the field's type, an error is returned.
func (dt *DbfTable) SetFieldValue(row int, fieldIndex int, value string) (err error) {
	b := []byte(dt.encoder.ConvertString(value))
	fieldLength := int(dt.fields[fieldIndex].length)

	offset := int(dt.numberOfBytesInHeader)
	lengthOfRecord := int(dt.lengthOfEachRecord)

	offset = offset + (row * lengthOfRecord)
	recordOffset := 1

	for i := 0; i < len(dt.fields); i++ {
		if i == fieldIndex {
			break
		} else {
			recordOffset += int(dt.fields[i].length)
		}
	}

	dt.fillFieldWithBlanks(fieldLength, offset, recordOffset)

	// write new value
	switch dt.fields[fieldIndex].fieldType {
	case Character, Logical, Date:
		for i := 0; i < len(b) && i < fieldLength; i++ {
			dt.dataStore[offset+recordOffset+i] = b[i]
		}
	case Float, Numeric:
		for i := 0; i < fieldLength; i++ {
			if i < len(b) {
				dt.dataStore[offset+recordOffset+(fieldLength-i-1)] = b[(len(b)-1)-i]
			} else {
				break
			}
		}
	}

	return

}

func (dt *DbfTable) fillFieldWithBlanks(fieldLength int, offset int, recordOffset int) {
	for i := 0; i < fieldLength; i++ {
		dt.dataStore[offset+recordOffset+i] = blank
	}
}

// FieldValue returns the content for the record at the given row and field index as a string
// If the row or field index is invalid, an error is returned .
func (dt *DbfTable) FieldValue(row int, fieldIndex int) (value string) {
	if fieldIndex == -1 {
		return
	}

	offset := int(dt.numberOfBytesInHeader)
	lengthOfRecord := int(dt.lengthOfEachRecord)

	offset = offset + (row * lengthOfRecord)

	recordOffset := 1

	for i := 0; i < len(dt.fields); i++ {
		if i == fieldIndex {
			break
		} else {
			recordOffset += int(dt.fields[i].length)
		}
	}

	temp := dt.dataStore[(offset + recordOffset):((offset + recordOffset) + int(dt.fields[fieldIndex].length))]

	enforceBlankPadding(temp)

	s := dt.decoder.ConvertString(string(temp))
	//fmt.Printf("utf-8 value:[%#v] original value:[%#v]\n", s, string(temp))

	value = strings.TrimSpace(s)
	// value = strings.TrimRight(s, " ")

	//fmt.Printf("raw value:[%#v]\n", dt.dataStore[(offset + recordOffset):((offset + recordOffset) + int(dt.Fields[fieldIndex].fixedFieldLength))])
	//fmt.Printf("utf-8 value:[%#v]\n", []byte(s))
	//value = string(dt.dataStore[(offset + recordOffset):((offset + recordOffset) + int(dt.Fields[fieldIndex].fixedFieldLength))])
	return
}

// Some Dbf encoders pad with null chars instead of blanks, this forces blanks as per
// https://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm
func enforceBlankPadding(temp []byte) {
	for i := 0; i < len(temp); i++ {
		if temp[i] == null {
			temp[i] = blank
		}
	}
}

// Float64FieldValueByName returns the value of a field given row number and name provided as a float64
func (dt *DbfTable) Float64FieldValueByName(row int, fieldName string) (value float64, err error) {
	valueAsString, err := dt.FieldValueByName(row, fieldName)
	return strconv.ParseFloat(valueAsString, 64)
}

// Int64FieldValueByName returns the value of a field given row number and name provided as an int64
func (dt *DbfTable) Int64FieldValueByName(row int, fieldName string) (value int64, err error) {
	valueAsString, err := dt.FieldValueByName(row, fieldName)
	return strconv.ParseInt(valueAsString, 0, 64)
}

// FieldValueByName returns the value of a field given row number and name provided
func (dt *DbfTable) FieldValueByName(row int, fieldName string) (value string, err error) {
	defer func() {
		if msg := recover(); msg != nil {
			value = ""
			err = fmt.Errorf("%v", msg)
		}
	}()
	if fieldIndex, entryFound := dt.fieldMap[fieldName]; entryFound {
		return dt.FieldValue(row, fieldIndex), err
	}
	err = errors.New("Field name \"" + fieldName + "\" does not exist")
	return
}

// RowIsDeleted returns whether a row has marked as deleted
func (dt *DbfTable) RowIsDeleted(row int) bool {
	if row < 0 || row >= int(dt.NumberOfRecords()) {
		return false
	}
	offset := int(dt.numberOfBytesInHeader)
	lengthOfRecord := int(dt.lengthOfEachRecord)
	offset = offset + (row * lengthOfRecord)
	if len(dt.dataStore) > (offset + 1) {
		return dt.dataStore[offset:(offset + 1)][recordDeletionFlagIndex] == recordIsDeleted
	}
	return false
}

// DeleteRow delete a row with specified row index
func (dt *DbfTable) DeleteRow(row int) error {
	if row < 0 || row >= int(dt.NumberOfRecords()) {
		return errors.New("out of range")
	}
	offset := int(dt.numberOfBytesInHeader) + (row * int(dt.lengthOfEachRecord))
	dt.dataStore[offset:(offset + 1)][recordDeletionFlagIndex] = recordIsDeleted
	return nil
}

// GetRowAsSlice return the record values for the row specified as a string slice
func (dt *DbfTable) GetRowAsSlice(row int) []string {
	s := make([]string, len(dt.Fields()))

	for i := 0; i < len(dt.Fields()); i++ {
		s[i] = dt.FieldValue(row, i)
	}

	return s
}

func (dt *DbfTable) Data() []byte {
	return dt.dataStore
}

func (dt *DbfTable) Dbase() []byte {
	return append(dt.dataStore, 0x1A)
}

// Copy return a new table that with schema unlocked
func (dt *DbfTable) Copy() *DbfTable {
	newDataSource := make([]byte, len(dt.dataStore))
	copy(newDataSource[:], dt.dataStore[:])
	newTable, _ := NewFromByteArray(newDataSource, dt.textEncoding)
	newTable.schemaLocked = false
	return newTable
}

// func formatValue(f FieldDescriptor, value string) string {
// 	left := func(str string, length int) string {
// 		runes := []rune(str)
// 		if len(runes) > length {
// 			return string(runes[:length])
// 		}
// 		return str
// 	}

// 	if value != "" {
// 		switch f.fieldType {
// 		case 'C':
// 			value = left(value, int(f.length))
// 		case 'N':
// 			if n, err := strconv.ParseFloat(value, 32); err == nil {
// 				value = fmt.Sprintf("%."+strconv.Itoa(int(f.decimalPlaces))+"f", n)
// 			}
// 		case 'D':
// 			switch f.format {
// 			case "", "20060102":
// 			case "RFC3339":
// 				t, _ := time.Parse(time.RFC3339, value)
// 				value = t.Format("20060102")
// 			default:
// 				t, _ := time.Parse(f.format, value)
// 				value = t.Format("20060102")
// 			}
// 		}
// 	}
// 	return value
// }

// func (dt *DbfTable) FormatValue(fieldName, value string) string {
// 	return formatValue(dt.FieldByName(fieldName), value)
// }

// Deprecated: Use SaveToFile() instead.
func (dt *DbfTable) SaveFile(filename string) error {
	return errors.New("godbf.DbfTable.SaveFile() is deprecated; Use godbf.SaveToFile() instead")
}
