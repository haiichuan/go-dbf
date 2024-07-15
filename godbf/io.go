// Package godbf offers functionality for loading and saving  "dBASE Version 5" dbf formatted files.
// (https://en.wikipedia.org/wiki/.dbf#File_format_of_Level_5_DOS_dBASE) file structure.
// For the definitive source, see http://www.dbase.com/manuals/57LanguageReference.zip
package godbf

import (
	"encoding/csv"
	"fmt"
	"os"
)

// NewFromFile creates a DbfTable, reading it from a file with the given file name, expecting the supplied encoding.
func NewFromFile(fileName string, fileEncoding string) (table *DbfTable, newErr error) {
	defer func() {
		if e := recover(); e != nil {
			newErr = fmt.Errorf("%v", e)
		}
	}()

	data, readErr := readFile(fileName)
	if readErr != nil {
		return nil, readErr
	}
	return NewFromByteArray(data, fileEncoding)
}

// SaveToFile saves the supplied DbfTable to a file of the specified filename
func SaveToFile(dt *DbfTable, filename string) (saveErr error) {
	defer func() {
		if e := recover(); e != nil {
			saveErr = fmt.Errorf("%v", e)
		}
	}()

	f, createErr := fsWrapper.Create(filename)
	if createErr != nil {
		return createErr
	}

	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			saveErr = closeErr
		}
	}()

	writeErr := writeContent(dt, f)
	if writeErr != nil {
		return writeErr
	}

	return saveErr
}

// SaveCSV translate dbf to csv format
func (dt *DbfTable) SaveCSV(filename string, delimiter rune, headers bool) (err error) {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(filename)
		}
	}()

	w := csv.NewWriter(f)
	w.Comma = delimiter
	if headers {
		fields := dt.Fields()
		fieldRow := make([]string, len(fields))
		for i := 0; i < len(fields); i++ {
			fieldRow[i] = fields[i].Name()
		}
		if err := w.Write(fieldRow); err != nil {
			return err
		}
		w.Flush()
	}

	for i := 0; i < dt.NumberOfRecords(); i++ {
		row := dt.GetRowAsSlice(i)
		if err := w.Write(row); err != nil {
			return err
		}
		w.Flush()
	}
	return
}

func writeContent(dt *DbfTable, f *os.File) error {
	if _, dsErr := f.Write(dt.dataStore); dsErr != nil {
		return dsErr
	}
	return nil
}
