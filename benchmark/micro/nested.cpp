#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;
using namespace std;

struct ListScanFunctionData : public TableFunctionData {
	ListScanFunctionData(size_t nrow, size_t list_size) : nrow(nrow), list_size(list_size), main_offset(0) {
	}

	size_t nrow;
	size_t list_size;
	size_t main_offset;
};

static unique_ptr<FunctionData> list_scan_bind(ClientContext &context, vector<Value> inputs,
                                               vector<SQLType> &return_types, vector<string> &names) {
	names.push_back("some_int");
	return_types.push_back(SQLType::INTEGER);

	names.push_back("some_list");
	auto list_type = SQLType::LIST;
	list_type.child_type.push_back(make_pair("", SQLType::INTEGER));
	return_types.push_back(list_type);

	assert(inputs.size() == 2);

	return make_unique<ListScanFunctionData>(inputs[0].GetValue<int32_t>(), inputs[1].GetValue<int32_t>());
}

void list_scan_function(ClientContext &context, vector<Value> &input, DataChunk &output, FunctionData *dataptr) {
	auto &data = *((ListScanFunctionData *)dataptr);
	// assert(input.size() == 0);

	if (data.nrow < 1) {
		return;
	}

	// generate data for two output columns
	size_t this_rows = std::min(data.nrow, (size_t)STANDARD_VECTOR_SIZE);
	data.nrow -= this_rows;
	output.SetCardinality(this_rows);

	auto int_data = (int32_t *)output.data[0].GetData();
	for (size_t row = 0; row < this_rows; row++) {
		int_data[row] = row + 1 + data.main_offset;
	}
	data.main_offset += this_rows;

	auto list_child = make_unique<ChunkCollection>();
	output.data[1].SetListEntry(move(list_child));
	auto list_data = (list_entry_t *)output.data[1].GetData();
	for (size_t row = 0; row < this_rows; row++) {
		idx_t rem_list_size = data.list_size;
		idx_t cur_list_offset = 0;
		list_data[row].offset = output.data[1].GetListEntry().count;
		list_data[row].length = data.list_size;

		DataChunk child_chunk;
		vector<TypeId> child_chunk_types;
		child_chunk_types.push_back(TypeId::INT32);
		child_chunk.Initialize(child_chunk_types);
		while (rem_list_size > 0) {
			size_t this_rows = std::min((size_t)rem_list_size, (size_t)STANDARD_VECTOR_SIZE);
			child_chunk.SetCardinality(this_rows);
			auto child_data = (int32_t *)child_chunk.data[0].GetData();
			for (size_t crow = 0; crow < this_rows; crow++) {
				child_data[crow] = cur_list_offset + crow + 1;
			}
			output.data[1].GetListEntry().Append(child_chunk);
			rem_list_size -= this_rows;
			cur_list_offset += this_rows;
		}
	}
}

class MyScanFunction : public TableFunction {
public:
	MyScanFunction()
	    : TableFunction("list_scan", {SQLTypeId::INTEGER, SQLTypeId::INTEGER}, list_scan_bind, list_scan_function,
	                    nullptr){};
};

#define NESTED_DATA_GEN_BODY(STRING_LENGTH)                                                                            \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		MyScanFunction scan_fun;                                                                                       \
		CreateTableFunctionInfo info(scan_fun);                                                                        \
		auto &context = *state->conn.context;                                                                          \
		context.transaction.SetAutoCommit(false);                                                                      \
		context.transaction.BeginTransaction();                                                                        \
		context.catalog.CreateTableFunction(context, &info);                                                           \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		/*result->Print();  */                                                                                         \
		if (!result->success) {                                                                                        \
			return result->error;                                                                                      \
		}                                                                                                              \
		return string();                                                                                               \
	}

// these test the unnest directly without list construction

DUCKDB_BENCHMARK(NestedOneUnnest, "[nested]")
NESTED_DATA_GEN_BODY(4)
string GetQuery() override {
	return "SELECT MAX(some_int), MAX(u) FROM (SELECT some_int, UNNEST(some_list) u from list_scan(100000, 1))sq";
}
string BenchmarkInfo() override {
	return "LIST UNNEST";
}
FINISH_BENCHMARK(NestedOneUnnest)

DUCKDB_BENCHMARK(NestedShortUnnest, "[nested]")
NESTED_DATA_GEN_BODY(4)
string GetQuery() override {
	return "SELECT MAX(some_int), MAX(u) FROM (SELECT some_int, UNNEST(some_list) u from list_scan(10000, 100))sq";
}
string BenchmarkInfo() override {
	return "LIST UNNEST";
}
FINISH_BENCHMARK(NestedShortUnnest)

DUCKDB_BENCHMARK(NestedLongUnnest, "[nested]")
NESTED_DATA_GEN_BODY(4)
string GetQuery() override {
	return "SELECT MAX(some_int), MAX(u) FROM (SELECT some_int, UNNEST(some_list) u from list_scan(10, 100000))sq";
}
string BenchmarkInfo() override {
	return "LIST UNNEST";
}
FINISH_BENCHMARK(NestedLongUnnest)

// todo list construction
