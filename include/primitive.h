#include "base2.h"


struct Int32Schema : public ArrowSchema2 {
    Int32Schema(std::string name) : ArrowSchema2("i", name) {}
};

struct NullableArray : public ArrowArray2 {
    NullableArray(size_t len) : ArrowArray2() {
        this->add_buffer<char>(len / 8 + 1);
    }

    char* get_bit_buf() {
        return this->get_buffer<char>(0);
    }
};

template<typename T>
struct PrimArray : public NullableArray {
    PrimArray(size_t len) : NullableArray(len) {
        this->add_buffer<T>(len);
    }

    T* get_val_buf() {
        return this->get_buffer<T>(1);
    }
};

using Int8Array = PrimArray<int8_t>;
using Int16Array = PrimArray<int16_t>;
using Int32Array = PrimArray<int32_t>;
using Int64Array = PrimArray<int64_t>;
using FloatArray = PrimArray<float>;
using DoubleArray = PrimArray<double>;

struct StructArray : public NullableArray {
    StructArray(size_t len) : NullableArray(len) {}
};
