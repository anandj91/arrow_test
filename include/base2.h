#include <cstring>

#include "base.h"

struct ArrowSchema2;
struct ArrowArray2;

struct PrivateData {
    int count;
    void* ptrs[100];

    PrivateData() : count(0) {}
};

struct ArrowSchema2 : public ArrowSchema {
    ArrowSchema2(
        std::string format,
        std::string name
    ) {
        arrow_make_schema(this);

        strcpy((char*) this->format, format.c_str());
        strcpy((char*) this->name, name.c_str());
        this->flags = ARROW_FLAG_NULLABLE;

        this->private_data = (void*) new PrivateData();
        this->release = (void(*)(ArrowSchema*)) &arrow_release_schema2;
    }

    static void arrow_release_schema2(ArrowSchema2* schema) {
        if (!schema->release) return;

        arrow_release_schema(schema);

        auto* pdata = (PrivateData*) schema->private_data;
        for (int i=0; i<pdata->count; i++) {
            delete (ArrowSchema2*) pdata->ptrs[i];
        }
        delete pdata;
    }

    ~ArrowSchema2() {
        arrow_release_schema2(this);
    }

    ArrowSchema2* add_child(std::string format, std::string name) {
        auto* schema = new ArrowSchema2(format, name);
        arrow_add_child_schema(this, schema);

        auto* pdata = (PrivateData*) this->private_data;
        pdata->ptrs[pdata->count] = schema;
        pdata->count++;

        return schema;
    }

    ArrowSchema* get_child(int idx) {
        return arrow_get_child_schema(this, idx);
    }
};

struct ArrowArray2 : public ArrowArray {
    ArrowArray2() {
        arrow_make_array(this);
        this->private_data = (void*) new PrivateData();
        this->release = (void(*)(ArrowArray*)) &arrow_release_array2;
    }

    static void arrow_release_array2(ArrowArray2* array) {
        if (!array->release) return;

        arrow_release_array(array);

        auto* pdata = (PrivateData*) array->private_data;
        for (int i=0; i<pdata->count; i++) {
            delete (ArrowArray2*) pdata->ptrs[i];
        }
        delete pdata;
    }

    ~ArrowArray2() {
        arrow_release_array2(this);
    }

    template<typename T, typename... Args>
    T* add_child(Args... args) {
        auto* array = new T(args...);
        arrow_add_child_array(this, array);

        auto* pdata = (PrivateData*) this->private_data;
        pdata->ptrs[pdata->count] = (void*) array;
        pdata->count++;

        return array;
    }

    template<typename T>
    T* add_buffer(size_t len) {
        return (T*) arrow_add_buffer(this, len * sizeof(T));
    }

    template<typename T>
    T* get_child(int idx) {
        return (T*) arrow_get_child_array(this, idx);
    }

    template<typename T>
    T* get_buffer(int idx) {
        return (T*) arrow_get_buffer(this, idx);
    }
};
