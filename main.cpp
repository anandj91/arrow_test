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

#include <iostream>
#include <vector>
#include <chrono>

#include "defs.h"

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/c/bridge.h>

using namespace std::chrono;

bool is_valid(char* bitmap, int j) {
    return bitmap[j / 8] & (1 << (j % 8));
}

void transform(ArrowSchema* in_schema, ArrowArray* in_array, StructSchema* out_schema, StructArray* out_array) {
    auto* in_i32_array = in_array->children[0];
    auto* in_i32_value = (int*) in_i32_array->buffers[1];

    out_schema->add_child<Int32Schema>("day");

    auto* out_bitmap = out_array->get_bit_buf();
    auto* out_i32_array = out_array->add_child<Int32Array>(in_i32_array->length);
    auto* out_i32_bitmap = out_i32_array->get_bit_buf();
    auto* out_i32_value = out_i32_array->get_val_buf();

    for (int i=0; i<in_i32_array->length; i++) {
        out_array->length++;
        out_i32_array->length++;
        out_bitmap[i/8] |= (1 << (i%8));
        out_i32_bitmap[i/8] |= (1 << (i%8));
        out_i32_value[i] = in_i32_value[i] + 10;
    }
}

arrow::Status manipulate(std::shared_ptr<arrow::RecordBatch> rbatch) {
    ArrowSchema in_schema;
    ArrowArray in_array;

    std::cout << "#### Before #### " << std::endl << rbatch->ToString() << std::endl;
    auto estart = high_resolution_clock::now();
    ARROW_RETURN_NOT_OK(arrow::ExportRecordBatch(*rbatch, &in_array, &in_schema));
    auto estop = high_resolution_clock::now();

    arrow_print_schema(&in_schema);
    arrow_print_array(&in_array);

    StructSchema out_schema("Date");
    StructArray out_array(in_array.length);

    auto tstart = high_resolution_clock::now();
    transform(&in_schema, &in_array, &out_schema, &out_array);
    auto tstop = high_resolution_clock::now();

    arrow_print_schema(&out_schema);
    arrow_print_array(&out_array);

    auto istart = high_resolution_clock::now();
    ARROW_ASSIGN_OR_RAISE(auto res, arrow::ImportRecordBatch(&out_array, &out_schema));
    auto istop = high_resolution_clock::now();

    std::cout << "Export time: " << duration_cast<microseconds>(estop - estart).count() << std::endl;
    std::cout << "Transform time: " << duration_cast<microseconds>(tstop - tstart).count() << std::endl;
    std::cout << "Import time: " << duration_cast<microseconds>(istop - istart).count() << std::endl;

    std::cout << std::endl;
    std::cout << "#### After #### " << std::endl << res->ToString() << std::endl;

    return arrow::Status::OK();
}

arrow::Status GenInitialFile() {
  // Make a couple 8-bit integer arrays and a 16-bit integer array -- just like
  // basic Arrow example.
  arrow::Int32Builder intbuilder;
  for (int i=0; i<1000; i++) {
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

  ARROW_RETURN_NOT_OK(manipulate(rbatch));

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
