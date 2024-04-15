// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/c/bridge.h>

#include <iostream>
#include <vector>
#include <chrono>

using namespace std::chrono;


#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

arrow::Status manipulate(std::shared_ptr<arrow::RecordBatch> rbatch) {
    ArrowSchema schema;
    ArrowArray array;
    auto start = high_resolution_clock::now();
    ARROW_RETURN_NOT_OK(arrow::ExportRecordBatch(*rbatch, &array, &schema));
    auto end = high_resolution_clock::now();

    std::cout << "Time: " << duration_cast<microseconds>(end - start).count() << std::endl;
    std::cout << "Name: "<< schema.name << std::endl;
    std::cout << "Format: " << schema.format << std::endl;
    std::cout << "Num of children: "<< schema.n_children << std::endl;

    return arrow::Status::OK();
}

arrow::Status GenInitialFile() {
  // Make a couple 8-bit integer arrays and a 16-bit integer array -- just like
  // basic Arrow example.
  arrow::Int32Builder intbuilder;
  int8_t days_raw[5] = {1, 12, 17, 23, 28};
  for (int i=0; i<100000000; i++) {
      ARROW_RETURN_NOT_OK(intbuilder.Append(i));
  }
  std::shared_ptr<arrow::Array> days;
  ARROW_ASSIGN_OR_RAISE(days, intbuilder.Finish());

  std::vector<std::shared_ptr<arrow::Array>> columns = {days};

  // Make a schema to initialize the Table with
  std::shared_ptr<arrow::Field> field_day;
  std::shared_ptr<arrow::Schema> schema;

  field_day = arrow::field("Day", arrow::int32());

  schema = arrow::schema({field_day});
  // With the schema and data, create a Table
  std::shared_ptr<arrow::Table> table;
  table = arrow::Table::Make(schema, columns);

  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.arrow"));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                        arrow::ipc::MakeFileWriter(outfile, schema));
  ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(ipc_writer->Close());

  return arrow::Status::OK();
}


arrow::Status RunMain(int argc, char** argv) {
  ARROW_RETURN_NOT_OK(GenInitialFile());

  // Get "test_in.arrow" into our file pointer
  ARROW_ASSIGN_OR_RAISE(auto infile, arrow::io::ReadableFile::Open(
                                    "test_in.arrow", arrow::default_memory_pool()));

  // Open up the file with the IPC features of the library, gives us a reader object.
  ARROW_ASSIGN_OR_RAISE(auto ipc_reader, arrow::ipc::RecordBatchFileReader::Open(infile));

  // Using the reader, we can read Record Batches. Note that this is specific to IPC;
  // for other formats, we focus on Tables, but here, RecordBatches are used.
  ARROW_ASSIGN_OR_RAISE(auto rbatch, ipc_reader->ReadRecordBatch(0));

  std::cout << "#### Before #### " << std::endl << rbatch->ToString() << std::endl;
  ARROW_RETURN_NOT_OK(manipulate(rbatch));
  std::cout << std::endl;
  std::cout << "#### After #### " << std::endl << rbatch->ToString() << std::endl;

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  arrow::Status status = RunMain(argc, argv);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
