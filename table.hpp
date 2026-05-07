#ifndef MORLOC_TABLE_HPP
#define MORLOC_TABLE_HPP

// table.hpp -- C++ implementations for the table stdlib module.
//
// Tables are mlc::ArrowTable wrappers around a pair of ArrowSchema +
// ArrowArray (Arrow C Data Interface). Operations rebuild the output table
// row-by-row using nanoarrow's typed view (read) and append (write) APIs.
//
// The supported column types match what the test fixtures and pyarrow's
// default-inferred schemas produce: int64 ('l'), float64 ('g'), and utf8
// ('u'). The int32 ('i') and float32 ('f') paths are also wired in so a
// CSV-typed schema or arrow-r's default integer (int32) flows through.

#include "mlc_arrow.hpp"
#include "morloc.h"

#include <nanoarrow/nanoarrow.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

namespace mlc_table {

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

inline void check(int rc, const char* what) {
    if (rc != NANOARROW_OK) {
        throw std::runtime_error(std::string("table-cpp: ") + what +
                                 " failed (code " + std::to_string(rc) + ")");
    }
}

inline int64_t find_column(const ArrowSchema* schema, const std::string& name) {
    for (int64_t i = 0; i < schema->n_children; ++i) {
        if (schema->children[i]->name && name == schema->children[i]->name) {
            return i;
        }
    }
    throw std::runtime_error("table-cpp: column '" + name + "' not found");
}

// Append the value at src_view[src_row] to dst_col, dispatching on the source
// schema's format string.
inline void copy_value(
    const ArrowArrayView* src_view,
    int64_t src_row,
    ArrowArray* dst_col,
    const char* type_format
) {
    char tag = type_format[0];
    switch (tag) {
        case 'i':
        case 'l': {
            int64_t v = ArrowArrayViewGetIntUnsafe(src_view, src_row);
            check(ArrowArrayAppendInt(dst_col, v), "ArrowArrayAppendInt");
            return;
        }
        case 'f':
        case 'g': {
            double v = ArrowArrayViewGetDoubleUnsafe(src_view, src_row);
            check(ArrowArrayAppendDouble(dst_col, v), "ArrowArrayAppendDouble");
            return;
        }
        case 'b': {
            int64_t v = ArrowArrayViewGetIntUnsafe(src_view, src_row);
            check(ArrowArrayAppendInt(dst_col, v), "ArrowArrayAppendInt");
            return;
        }
        case 'u': {
            ArrowStringView sv = ArrowArrayViewGetStringUnsafe(src_view, src_row);
            check(ArrowArrayAppendString(dst_col, sv), "ArrowArrayAppendString");
            return;
        }
        default:
            throw std::runtime_error(
                std::string("table-cpp: unsupported column type '") +
                type_format + "'");
    }
}

struct ViewGuard {
    ArrowArrayView view;
    bool initialized;
    ViewGuard() : initialized(false) {
        memset(&view, 0, sizeof(view));
    }
    ~ViewGuard() {
        if (initialized) ArrowArrayViewReset(&view);
    }
    ViewGuard(const ViewGuard&) = delete;
    ViewGuard& operator=(const ViewGuard&) = delete;
};

inline void make_struct_schema(
    ArrowSchema* out,
    const std::vector<const ArrowSchema*>& col_schemas
) {
    memset(out, 0, sizeof(*out));
    check(ArrowSchemaInitFromType(out, NANOARROW_TYPE_STRUCT),
          "ArrowSchemaInitFromType (struct)");
    check(ArrowSchemaAllocateChildren(out, (int64_t)col_schemas.size()),
          "ArrowSchemaAllocateChildren");
    for (size_t i = 0; i < col_schemas.size(); ++i) {
        check(ArrowSchemaDeepCopy(col_schemas[i], out->children[i]),
              "ArrowSchemaDeepCopy");
    }
}

// Take a subset of rows in `indices` order, preserving every column.
inline mlc::ArrowTable table_gather_rows(
    const mlc::ArrowTable& src,
    const std::vector<int64_t>& indices
) {
    const ArrowSchema* in_schema = src.schema();
    const ArrowArray*  in_array  = src.array();
    int64_t n_cols = in_schema->n_children;

    ArrowSchema out_schema;
    memset(&out_schema, 0, sizeof(out_schema));
    check(ArrowSchemaDeepCopy(in_schema, &out_schema), "ArrowSchemaDeepCopy");

    ViewGuard vg;
    check(ArrowArrayViewInitFromSchema(&vg.view, in_schema, nullptr),
          "ArrowArrayViewInitFromSchema");
    vg.initialized = true;
    check(ArrowArrayViewSetArray(&vg.view, in_array, nullptr),
          "ArrowArrayViewSetArray");

    ArrowArray out_array;
    memset(&out_array, 0, sizeof(out_array));
    check(ArrowArrayInitFromSchema(&out_array, &out_schema, nullptr),
          "ArrowArrayInitFromSchema");
    check(ArrowArrayStartAppending(&out_array),
          "ArrowArrayStartAppending");

    for (int64_t row : indices) {
        for (int64_t c = 0; c < n_cols; ++c) {
            copy_value(vg.view.children[c], row,
                       out_array.children[c],
                       in_schema->children[c]->format);
        }
        check(ArrowArrayFinishElement(&out_array),
              "ArrowArrayFinishElement");
    }

    ArrowError err;
    check(ArrowArrayFinishBuildingDefault(&out_array, &err),
          "ArrowArrayFinishBuildingDefault");

    return mlc::ArrowTable(out_schema, out_array);
}

// Take a subset of columns in `col_indices` order, preserving every row.
inline mlc::ArrowTable table_gather_cols(
    const mlc::ArrowTable& src,
    const std::vector<int64_t>& col_indices
) {
    const ArrowSchema* in_schema = src.schema();
    const ArrowArray*  in_array  = src.array();
    int64_t n_rows = in_array->length;

    std::vector<const ArrowSchema*> kept_schemas;
    kept_schemas.reserve(col_indices.size());
    for (int64_t i : col_indices) kept_schemas.push_back(in_schema->children[i]);

    ArrowSchema out_schema;
    make_struct_schema(&out_schema, kept_schemas);

    ViewGuard vg;
    check(ArrowArrayViewInitFromSchema(&vg.view, in_schema, nullptr),
          "ArrowArrayViewInitFromSchema");
    vg.initialized = true;
    check(ArrowArrayViewSetArray(&vg.view, in_array, nullptr),
          "ArrowArrayViewSetArray");

    ArrowArray out_array;
    memset(&out_array, 0, sizeof(out_array));
    check(ArrowArrayInitFromSchema(&out_array, &out_schema, nullptr),
          "ArrowArrayInitFromSchema");
    check(ArrowArrayStartAppending(&out_array),
          "ArrowArrayStartAppending");

    for (int64_t row = 0; row < n_rows; ++row) {
        for (size_t k = 0; k < col_indices.size(); ++k) {
            int64_t c = col_indices[k];
            copy_value(vg.view.children[c], row,
                       out_array.children[k],
                       in_schema->children[c]->format);
        }
        check(ArrowArrayFinishElement(&out_array),
              "ArrowArrayFinishElement");
    }

    ArrowError err;
    check(ArrowArrayFinishBuildingDefault(&out_array, &err),
          "ArrowArrayFinishBuildingDefault");

    return mlc::ArrowTable(out_schema, out_array);
}

// Per-element type machinery for asCol / setCol / getCol.

template <class T>
inline const char* arrow_format_for() {
    static_assert(sizeof(T) == 0, "table-cpp: unsupported column type");
    return nullptr;
}
template <> inline const char* arrow_format_for<int64_t>()    { return "l"; }
template <> inline const char* arrow_format_for<int>()        { return "i"; }
template <> inline const char* arrow_format_for<double>()     { return "g"; }
template <> inline const char* arrow_format_for<float>()      { return "f"; }
template <> inline const char* arrow_format_for<bool>()       { return "b"; }
template <> inline const char* arrow_format_for<std::string>(){ return "u"; }

template <class T>
inline void append_typed(ArrowArray* col, const T& value) {
    static_assert(sizeof(T) == 0, "table-cpp: unsupported column type");
}
template <>
inline void append_typed<int64_t>(ArrowArray* col, const int64_t& value) {
    check(ArrowArrayAppendInt(col, value), "ArrowArrayAppendInt");
}
template <>
inline void append_typed<int>(ArrowArray* col, const int& value) {
    check(ArrowArrayAppendInt(col, (int64_t)value), "ArrowArrayAppendInt");
}
template <>
inline void append_typed<double>(ArrowArray* col, const double& value) {
    check(ArrowArrayAppendDouble(col, value), "ArrowArrayAppendDouble");
}
template <>
inline void append_typed<float>(ArrowArray* col, const float& value) {
    check(ArrowArrayAppendDouble(col, (double)value), "ArrowArrayAppendDouble");
}
template <>
inline void append_typed<bool>(ArrowArray* col, const bool& value) {
    check(ArrowArrayAppendInt(col, value ? 1 : 0), "ArrowArrayAppendInt");
}
template <>
inline void append_typed<std::string>(ArrowArray* col, const std::string& value) {
    ArrowStringView sv;
    sv.data = value.c_str();
    sv.size_bytes = (int64_t)value.size();
    check(ArrowArrayAppendString(col, sv), "ArrowArrayAppendString");
}

template <class T>
inline T read_typed(const ArrowArrayView* view, int64_t row) {
    static_assert(sizeof(T) == 0, "table-cpp: unsupported column type");
}
template <>
inline int64_t read_typed<int64_t>(const ArrowArrayView* view, int64_t row) {
    return ArrowArrayViewGetIntUnsafe(view, row);
}
template <>
inline int read_typed<int>(const ArrowArrayView* view, int64_t row) {
    return (int)ArrowArrayViewGetIntUnsafe(view, row);
}
template <>
inline double read_typed<double>(const ArrowArrayView* view, int64_t row) {
    return ArrowArrayViewGetDoubleUnsafe(view, row);
}
template <>
inline float read_typed<float>(const ArrowArrayView* view, int64_t row) {
    return (float)ArrowArrayViewGetDoubleUnsafe(view, row);
}
template <>
inline bool read_typed<bool>(const ArrowArrayView* view, int64_t row) {
    return ArrowArrayViewGetIntUnsafe(view, row) != 0;
}
template <>
inline std::string read_typed<std::string>(const ArrowArrayView* view, int64_t row) {
    ArrowStringView sv = ArrowArrayViewGetStringUnsafe(view, row);
    return std::string(sv.data, (size_t)sv.size_bytes);
}

} // namespace mlc_table


// ===========================================================================
// Public API: the morloc operations sourced by `table-cpp/main.loc`.
// ===========================================================================

// Construction: single-column table.
template <class T>
inline mlc::ArrowTable morloc_asCol(
    const std::string& colname,
    const std::vector<T>& vec
) {
    const char* fmt = mlc_table::arrow_format_for<T>();
    int64_t n = (int64_t)vec.size();

    ArrowSchema out_schema;
    memset(&out_schema, 0, sizeof(out_schema));
    mlc_table::check(ArrowSchemaInitFromType(&out_schema, NANOARROW_TYPE_STRUCT),
                     "ArrowSchemaInitFromType (struct)");
    mlc_table::check(ArrowSchemaAllocateChildren(&out_schema, 1),
                     "ArrowSchemaAllocateChildren");
    ArrowSchemaInit(out_schema.children[0]);
    mlc_table::check(ArrowSchemaSetFormat(out_schema.children[0], fmt),
                     "ArrowSchemaSetFormat");
    mlc_table::check(ArrowSchemaSetName(out_schema.children[0], colname.c_str()),
                     "ArrowSchemaSetName");

    ArrowArray out_array;
    memset(&out_array, 0, sizeof(out_array));
    mlc_table::check(ArrowArrayInitFromSchema(&out_array, &out_schema, nullptr),
                     "ArrowArrayInitFromSchema");
    mlc_table::check(ArrowArrayStartAppending(&out_array),
                     "ArrowArrayStartAppending");

    for (int64_t i = 0; i < n; ++i) {
        mlc_table::append_typed<T>(out_array.children[0], vec[i]);
        mlc_table::check(ArrowArrayFinishElement(&out_array),
                         "ArrowArrayFinishElement");
    }

    ArrowError err;
    mlc_table::check(ArrowArrayFinishBuildingDefault(&out_array, &err),
                     "ArrowArrayFinishBuildingDefault");

    return mlc::ArrowTable(out_schema, out_array);
}


// Introspection.

inline int64_t morloc_nrow(const mlc::ArrowTable& t) { return t.n_rows(); }
inline int64_t morloc_ncol(const mlc::ArrowTable& t) { return t.n_columns(); }

inline std::vector<std::string> morloc_names(const mlc::ArrowTable& t) {
    const ArrowSchema* schema = t.schema();
    std::vector<std::string> out;
    out.reserve((size_t)schema->n_children);
    for (int64_t i = 0; i < schema->n_children; ++i) {
        const char* n = schema->children[i]->name;
        out.emplace_back(n ? n : "");
    }
    return out;
}


// Row operations.

inline mlc::ArrowTable morloc_sliceRows(int64_t start, int64_t end, const mlc::ArrowTable& src) {
    int64_t total = src.n_rows();
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if (start >= total || end <= start) {
        std::vector<int64_t> empty;
        return mlc_table::table_gather_rows(src, empty);
    }
    int64_t end_clamped = std::min(end, total);
    int64_t take = end_clamped - start;
    std::vector<int64_t> idx;
    idx.reserve((size_t)take);
    for (int64_t i = 0; i < take; ++i) idx.push_back(start + i);
    return mlc_table::table_gather_rows(src, idx);
}

inline mlc::ArrowTable morloc_filterRows(
    const std::vector<bool>& mask,
    const mlc::ArrowTable& src
) {
    int64_t total = src.n_rows();
    if ((int64_t)mask.size() != total) {
        throw std::runtime_error(
            "table-cpp: filterRows mask length (" + std::to_string(mask.size()) +
            ") does not match table row count (" + std::to_string(total) + ")");
    }
    std::vector<int64_t> idx;
    for (int64_t i = 0; i < total; ++i) {
        if (mask[(size_t)i]) idx.push_back(i);
    }
    return mlc_table::table_gather_rows(src, idx);
}

inline mlc::ArrowTable morloc_pickRows(
    const std::vector<int>& indices,
    const mlc::ArrowTable& src
) {
    std::vector<int64_t> idx64;
    idx64.reserve(indices.size());
    for (int v : indices) idx64.push_back((int64_t)v);
    return mlc_table::table_gather_rows(src, idx64);
}

// Distinct via row-tuple hash. We project each row into a string key
// (column-by-column, separated by an unlikely byte) and keep the first
// index for each unique key.
inline mlc::ArrowTable morloc_distinctRows(const mlc::ArrowTable& src) {
    const ArrowSchema* schema = src.schema();
    const ArrowArray*  array  = src.array();
    int64_t n_rows = array->length;
    int64_t n_cols = schema->n_children;
    if (n_rows <= 1) return mlc_table::table_gather_rows(src, n_rows == 1 ? std::vector<int64_t>{0} : std::vector<int64_t>{});

    mlc_table::ViewGuard vg;
    mlc_table::check(ArrowArrayViewInitFromSchema(&vg.view, schema, nullptr),
                     "ArrowArrayViewInitFromSchema");
    vg.initialized = true;
    mlc_table::check(ArrowArrayViewSetArray(&vg.view, array, nullptr),
                     "ArrowArrayViewSetArray");

    auto row_key = [&](int64_t row) -> std::string {
        std::string key;
        for (int64_t c = 0; c < n_cols; ++c) {
            char tag = schema->children[c]->format[0];
            const ArrowArrayView* cv = vg.view.children[c];
            switch (tag) {
                case 'i':
                case 'l':
                case 'b': {
                    int64_t v = ArrowArrayViewGetIntUnsafe(cv, row);
                    key.append(reinterpret_cast<const char*>(&v), sizeof(v));
                    break;
                }
                case 'f':
                case 'g': {
                    double v = ArrowArrayViewGetDoubleUnsafe(cv, row);
                    key.append(reinterpret_cast<const char*>(&v), sizeof(v));
                    break;
                }
                case 'u': {
                    ArrowStringView sv = ArrowArrayViewGetStringUnsafe(cv, row);
                    int64_t len = sv.size_bytes;
                    key.append(reinterpret_cast<const char*>(&len), sizeof(len));
                    key.append(sv.data, (size_t)sv.size_bytes);
                    break;
                }
                default:
                    throw std::runtime_error(
                        std::string("table-cpp: distinctRows unsupported type '") +
                        schema->children[c]->format + "'");
            }
            key.push_back('\x1f');
        }
        return key;
    };

    std::vector<int64_t> keep;
    keep.reserve((size_t)n_rows);
    std::vector<std::string> seen;
    seen.reserve((size_t)n_rows);
    for (int64_t r = 0; r < n_rows; ++r) {
        std::string k = row_key(r);
        bool found = false;
        for (const auto& s : seen) {
            if (s == k) { found = true; break; }
        }
        if (!found) {
            seen.push_back(std::move(k));
            keep.push_back(r);
        }
    }
    return mlc_table::table_gather_rows(src, keep);
}

// Multi-key stable sort. spec is [(name, ascending)].
inline mlc::ArrowTable morloc_sortRows(
    const std::vector<std::tuple<std::string, bool>>& spec,
    const mlc::ArrowTable& src
) {
    int64_t n = src.n_rows();
    std::vector<int64_t> idx(n);
    std::iota(idx.begin(), idx.end(), 0);
    if (spec.empty()) return mlc_table::table_gather_rows(src, idx);

    const ArrowSchema* schema = src.schema();
    const ArrowArray*  array  = src.array();

    mlc_table::ViewGuard vg;
    mlc_table::check(ArrowArrayViewInitFromSchema(&vg.view, schema, nullptr),
                     "ArrowArrayViewInitFromSchema");
    vg.initialized = true;
    mlc_table::check(ArrowArrayViewSetArray(&vg.view, array, nullptr),
                     "ArrowArrayViewSetArray");

    // Resolve column indices and tags up-front.
    std::vector<int64_t> cols;
    std::vector<bool> asc_flags;
    std::vector<char> tags;
    for (const auto& s : spec) {
        int64_t c = mlc_table::find_column(schema, std::get<0>(s));
        cols.push_back(c);
        asc_flags.push_back(std::get<1>(s));
        tags.push_back(schema->children[c]->format[0]);
    }

    auto cmp = [&](int64_t a, int64_t b) -> bool {
        for (size_t k = 0; k < cols.size(); ++k) {
            int64_t c = cols[k];
            const ArrowArrayView* cv = vg.view.children[c];
            int order = 0;
            switch (tags[k]) {
                case 'i':
                case 'l':
                case 'b': {
                    int64_t va = ArrowArrayViewGetIntUnsafe(cv, a);
                    int64_t vb = ArrowArrayViewGetIntUnsafe(cv, b);
                    order = (va < vb) ? -1 : (va > vb ? 1 : 0);
                    break;
                }
                case 'f':
                case 'g': {
                    double va = ArrowArrayViewGetDoubleUnsafe(cv, a);
                    double vb = ArrowArrayViewGetDoubleUnsafe(cv, b);
                    order = (va < vb) ? -1 : (va > vb ? 1 : 0);
                    break;
                }
                case 'u': {
                    ArrowStringView sa = ArrowArrayViewGetStringUnsafe(cv, a);
                    ArrowStringView sb = ArrowArrayViewGetStringUnsafe(cv, b);
                    int64_t m = std::min(sa.size_bytes, sb.size_bytes);
                    int co = memcmp(sa.data, sb.data, (size_t)m);
                    if (co != 0) order = co < 0 ? -1 : 1;
                    else order = (sa.size_bytes < sb.size_bytes) ? -1
                              : (sa.size_bytes > sb.size_bytes ? 1 : 0);
                    break;
                }
                default:
                    throw std::runtime_error(
                        std::string("table-cpp: sortRows unsupported key type '") +
                        schema->children[c]->format + "'");
            }
            if (order != 0) return asc_flags[k] ? (order < 0) : (order > 0);
        }
        return false;
    };

    std::stable_sort(idx.begin(), idx.end(), cmp);
    return mlc_table::table_gather_rows(src, idx);
}


// Column operations.

// getCol is determined by the *result* type, not by any argument, so a
// plain function template would fail return-type deduction. Return a small
// proxy holding the source table + column index; the templated conversion
// operator is what the morloc-generated assignment site instantiates,
// using the LHS std::vector<T> to pick T.
namespace mlc_table {

struct GetColProxy {
    const mlc::ArrowTable* src;
    int64_t col;

    template <class T>
    operator std::vector<T>() const {
        const ArrowSchema* schema = src->schema();
        const ArrowArray*  array  = src->array();
        int64_t n = array->length;

        ViewGuard vg;
        check(ArrowArrayViewInitFromSchema(&vg.view, schema, nullptr),
              "ArrowArrayViewInitFromSchema");
        vg.initialized = true;
        check(ArrowArrayViewSetArray(&vg.view, array, nullptr),
              "ArrowArrayViewSetArray");

        std::vector<T> out;
        out.reserve((size_t)n);
        for (int64_t i = 0; i < n; ++i) {
            out.push_back(read_typed<T>(vg.view.children[col], i));
        }
        return out;
    }
};

}  // namespace mlc_table

inline mlc_table::GetColProxy morloc_getCol(
    const std::string& name,
    const mlc::ArrowTable& src
) {
    int64_t col = mlc_table::find_column(src.schema(), name);
    return mlc_table::GetColProxy{&src, col};
}


template <class T>
inline mlc::ArrowTable morloc_setCol(
    const std::string& name,
    const std::vector<T>& vec,
    const mlc::ArrowTable& src
) {
    const ArrowSchema* in_schema = src.schema();
    int64_t n_rows = src.n_rows();
    int64_t n_cols = in_schema->n_children;
    const char* new_format = mlc_table::arrow_format_for<T>();

    int64_t replace_at = -1;
    for (int64_t i = 0; i < n_cols; ++i) {
        const char* cn = in_schema->children[i]->name;
        if (cn && name == cn) { replace_at = i; break; }
    }

    std::vector<const ArrowSchema*> kept;
    kept.reserve((size_t)(n_cols + 1));

    ArrowSchema single_col;
    memset(&single_col, 0, sizeof(single_col));
    mlc_table::check(ArrowSchemaInitFromType(&single_col, NANOARROW_TYPE_STRUCT),
                     "ArrowSchemaInitFromType (struct)");
    mlc_table::check(ArrowSchemaAllocateChildren(&single_col, 1),
                     "ArrowSchemaAllocateChildren");
    ArrowSchemaInit(single_col.children[0]);
    mlc_table::check(ArrowSchemaSetFormat(single_col.children[0], new_format),
                     "ArrowSchemaSetFormat");
    mlc_table::check(ArrowSchemaSetName(single_col.children[0], name.c_str()),
                     "ArrowSchemaSetName");

    for (int64_t i = 0; i < n_cols; ++i) {
        if (i == replace_at) kept.push_back(single_col.children[0]);
        else                  kept.push_back(in_schema->children[i]);
    }
    if (replace_at < 0) kept.push_back(single_col.children[0]);

    ArrowSchema out_schema;
    mlc_table::make_struct_schema(&out_schema, kept);
    if (single_col.release) single_col.release(&single_col);

    mlc_table::ViewGuard vg;
    mlc_table::check(ArrowArrayViewInitFromSchema(&vg.view, in_schema, nullptr),
                     "ArrowArrayViewInitFromSchema");
    vg.initialized = true;
    mlc_table::check(ArrowArrayViewSetArray(&vg.view, src.array(), nullptr),
                     "ArrowArrayViewSetArray");

    ArrowArray out_array;
    memset(&out_array, 0, sizeof(out_array));
    mlc_table::check(ArrowArrayInitFromSchema(&out_array, &out_schema, nullptr),
                     "ArrowArrayInitFromSchema");
    mlc_table::check(ArrowArrayStartAppending(&out_array),
                     "ArrowArrayStartAppending");

    if ((int64_t)vec.size() != n_rows) {
        throw std::runtime_error(
            "table-cpp: setCol row-count mismatch (vec=" +
            std::to_string(vec.size()) + ", table=" + std::to_string(n_rows) + ")");
    }

    for (int64_t row = 0; row < n_rows; ++row) {
        if (replace_at >= 0) {
            for (int64_t c = 0; c < n_cols; ++c) {
                if (c == replace_at) {
                    mlc_table::append_typed<T>(out_array.children[c], vec[row]);
                } else {
                    mlc_table::copy_value(vg.view.children[c], row,
                                          out_array.children[c],
                                          in_schema->children[c]->format);
                }
            }
        } else {
            for (int64_t c = 0; c < n_cols; ++c) {
                mlc_table::copy_value(vg.view.children[c], row,
                                      out_array.children[c],
                                      in_schema->children[c]->format);
            }
            mlc_table::append_typed<T>(out_array.children[n_cols], vec[row]);
        }
        mlc_table::check(ArrowArrayFinishElement(&out_array),
                         "ArrowArrayFinishElement");
    }

    ArrowError err;
    mlc_table::check(ArrowArrayFinishBuildingDefault(&out_array, &err),
                     "ArrowArrayFinishBuildingDefault");

    return mlc::ArrowTable(out_schema, out_array);
}


// dropCols: drop several columns by name. Drop-of-absent is benign.
inline mlc::ArrowTable morloc_dropCols(
    const std::vector<std::string>& names,
    const mlc::ArrowTable& src
) {
    const ArrowSchema* schema = src.schema();
    std::vector<int64_t> col_idx;
    col_idx.reserve((size_t)schema->n_children);
    for (int64_t i = 0; i < schema->n_children; ++i) {
        const char* cn = schema->children[i]->name;
        if (!cn) { col_idx.push_back(i); continue; }
        bool drop = false;
        for (const auto& n : names) {
            if (n == cn) { drop = true; break; }
        }
        if (!drop) col_idx.push_back(i);
    }
    return mlc_table::table_gather_cols(src, col_idx);
}


// selectCols: project to the named columns in the requested order.
inline mlc::ArrowTable morloc_selectCols(
    const std::vector<std::string>& names,
    const mlc::ArrowTable& src
) {
    std::vector<int64_t> col_idx;
    col_idx.reserve(names.size());
    for (const auto& n : names) col_idx.push_back(mlc_table::find_column(src.schema(), n));
    return mlc_table::table_gather_cols(src, col_idx);
}


// selectColsDyn: same kernel as selectCols; the type-level difference is
// purely about the schema reflected in the result type.
inline mlc::ArrowTable morloc_selectColsDyn(
    const std::vector<std::string>& names,
    const mlc::ArrowTable& src
) {
    return morloc_selectCols(names, src);
}


// renameCol: rebuild the schema with one column name swapped. Data
// arrays are untouched; the rebuild copies to honor ownership.
inline mlc::ArrowTable morloc_renameCol(
    const std::string& old_name,
    const std::string& new_name,
    const mlc::ArrowTable& src
) {
    const ArrowSchema* in_schema = src.schema();
    const ArrowArray*  in_array  = src.array();
    int64_t n_cols = in_schema->n_children;
    int64_t n_rows = in_array->length;

    int64_t target = mlc_table::find_column(in_schema, old_name);

    // Build a deep-copied schema and overwrite the target name.
    ArrowSchema out_schema;
    memset(&out_schema, 0, sizeof(out_schema));
    mlc_table::check(ArrowSchemaDeepCopy(in_schema, &out_schema), "ArrowSchemaDeepCopy");
    mlc_table::check(ArrowSchemaSetName(out_schema.children[target], new_name.c_str()),
                     "ArrowSchemaSetName");

    // Rebuild data row-by-row to preserve clean ownership semantics.
    mlc_table::ViewGuard vg;
    mlc_table::check(ArrowArrayViewInitFromSchema(&vg.view, in_schema, nullptr),
                     "ArrowArrayViewInitFromSchema");
    vg.initialized = true;
    mlc_table::check(ArrowArrayViewSetArray(&vg.view, in_array, nullptr),
                     "ArrowArrayViewSetArray");

    ArrowArray out_array;
    memset(&out_array, 0, sizeof(out_array));
    mlc_table::check(ArrowArrayInitFromSchema(&out_array, &out_schema, nullptr),
                     "ArrowArrayInitFromSchema");
    mlc_table::check(ArrowArrayStartAppending(&out_array),
                     "ArrowArrayStartAppending");

    for (int64_t row = 0; row < n_rows; ++row) {
        for (int64_t c = 0; c < n_cols; ++c) {
            mlc_table::copy_value(vg.view.children[c], row,
                                  out_array.children[c],
                                  in_schema->children[c]->format);
        }
        mlc_table::check(ArrowArrayFinishElement(&out_array),
                         "ArrowArrayFinishElement");
    }

    ArrowError err;
    mlc_table::check(ArrowArrayFinishBuildingDefault(&out_array, &err),
                     "ArrowArrayFinishBuildingDefault");

    return mlc::ArrowTable(out_schema, out_array);
}


// rbind: vertical concat. Both tables must share a column schema; the
// result's row count is n1 + n2. Schema is taken from t1 (t2 is asserted
// at the type level to match).
inline mlc::ArrowTable morloc_rbind(const mlc::ArrowTable& a, const mlc::ArrowTable& b) {
    const ArrowSchema* schema = a.schema();
    int64_t n_cols = schema->n_children;
    int64_t na = a.n_rows();
    int64_t nb = b.n_rows();

    ArrowSchema out_schema;
    memset(&out_schema, 0, sizeof(out_schema));
    mlc_table::check(ArrowSchemaDeepCopy(schema, &out_schema), "ArrowSchemaDeepCopy");

    mlc_table::ViewGuard va, vb;
    mlc_table::check(ArrowArrayViewInitFromSchema(&va.view, schema, nullptr),
                     "ArrowArrayViewInitFromSchema (a)");
    va.initialized = true;
    mlc_table::check(ArrowArrayViewSetArray(&va.view, a.array(), nullptr),
                     "ArrowArrayViewSetArray (a)");
    mlc_table::check(ArrowArrayViewInitFromSchema(&vb.view, b.schema(), nullptr),
                     "ArrowArrayViewInitFromSchema (b)");
    vb.initialized = true;
    mlc_table::check(ArrowArrayViewSetArray(&vb.view, b.array(), nullptr),
                     "ArrowArrayViewSetArray (b)");

    ArrowArray out_array;
    memset(&out_array, 0, sizeof(out_array));
    mlc_table::check(ArrowArrayInitFromSchema(&out_array, &out_schema, nullptr),
                     "ArrowArrayInitFromSchema");
    mlc_table::check(ArrowArrayStartAppending(&out_array),
                     "ArrowArrayStartAppending");

    for (int64_t row = 0; row < na; ++row) {
        for (int64_t c = 0; c < n_cols; ++c) {
            mlc_table::copy_value(va.view.children[c], row,
                                  out_array.children[c],
                                  schema->children[c]->format);
        }
        mlc_table::check(ArrowArrayFinishElement(&out_array),
                         "ArrowArrayFinishElement");
    }
    for (int64_t row = 0; row < nb; ++row) {
        for (int64_t c = 0; c < n_cols; ++c) {
            mlc_table::copy_value(vb.view.children[c], row,
                                  out_array.children[c],
                                  schema->children[c]->format);
        }
        mlc_table::check(ArrowArrayFinishElement(&out_array),
                         "ArrowArrayFinishElement");
    }

    ArrowError err;
    mlc_table::check(ArrowArrayFinishBuildingDefault(&out_array, &err),
                     "ArrowArrayFinishBuildingDefault");

    return mlc::ArrowTable(out_schema, out_array);
}


// cbind: horizontal concat. Both tables must share a row count; the result
// has the union of columns in left-then-right order.
inline mlc::ArrowTable morloc_cbind(const mlc::ArrowTable& a, const mlc::ArrowTable& b) {
    const ArrowSchema* sa = a.schema();
    const ArrowSchema* sb = b.schema();
    int64_t na = a.n_rows();
    int64_t nca = sa->n_children;
    int64_t ncb = sb->n_children;

    std::vector<const ArrowSchema*> cols;
    cols.reserve((size_t)(nca + ncb));
    for (int64_t i = 0; i < nca; ++i) cols.push_back(sa->children[i]);
    for (int64_t i = 0; i < ncb; ++i) cols.push_back(sb->children[i]);

    ArrowSchema out_schema;
    mlc_table::make_struct_schema(&out_schema, cols);

    mlc_table::ViewGuard va, vb;
    mlc_table::check(ArrowArrayViewInitFromSchema(&va.view, sa, nullptr),
                     "ArrowArrayViewInitFromSchema (a)");
    va.initialized = true;
    mlc_table::check(ArrowArrayViewSetArray(&va.view, a.array(), nullptr),
                     "ArrowArrayViewSetArray (a)");
    mlc_table::check(ArrowArrayViewInitFromSchema(&vb.view, sb, nullptr),
                     "ArrowArrayViewInitFromSchema (b)");
    vb.initialized = true;
    mlc_table::check(ArrowArrayViewSetArray(&vb.view, b.array(), nullptr),
                     "ArrowArrayViewSetArray (b)");

    ArrowArray out_array;
    memset(&out_array, 0, sizeof(out_array));
    mlc_table::check(ArrowArrayInitFromSchema(&out_array, &out_schema, nullptr),
                     "ArrowArrayInitFromSchema");
    mlc_table::check(ArrowArrayStartAppending(&out_array),
                     "ArrowArrayStartAppending");

    for (int64_t row = 0; row < na; ++row) {
        for (int64_t c = 0; c < nca; ++c) {
            mlc_table::copy_value(va.view.children[c], row,
                                  out_array.children[c],
                                  sa->children[c]->format);
        }
        for (int64_t c = 0; c < ncb; ++c) {
            mlc_table::copy_value(vb.view.children[c], row,
                                  out_array.children[nca + c],
                                  sb->children[c]->format);
        }
        mlc_table::check(ArrowArrayFinishElement(&out_array),
                         "ArrowArrayFinishElement");
    }

    ArrowError err;
    mlc_table::check(ArrowArrayFinishBuildingDefault(&out_array, &err),
                     "ArrowArrayFinishBuildingDefault");

    return mlc::ArrowTable(out_schema, out_array);
}


#endif  // MORLOC_TABLE_HPP
