#include "duckdb/execution/operator/projection/physical_unnest.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"

using namespace duckdb;
using namespace std;

//! The operator state of the window
class PhysicalUnnestOperatorState : public PhysicalOperatorState {
public:
	PhysicalUnnestOperatorState(PhysicalOperator *child)
	    : PhysicalOperatorState(child), parent_position(0), list_position(0), list_length(-1) {
	}

	idx_t parent_position;
	idx_t list_position;
	int64_t list_length = -1;

	DataChunk list_data;
};

// this implements a sorted window functions variant
PhysicalUnnest::PhysicalUnnest(LogicalOperator &op, vector<unique_ptr<Expression>> select_list,
                               PhysicalOperatorType type)
    : PhysicalOperator(type, op.types), select_list(std::move(select_list)) {
	assert(this->select_list.size() > 0);
}

void PhysicalUnnest::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalUnnestOperatorState *>(state_);
	while (true) { // repeat until we actually have produced some rows
		if (state->child_chunk.size() == 0 || state->parent_position >= state->child_chunk.size()) {
			// get the child data
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				return;
			}
			state->parent_position = 0;
			state->list_position = 0;
			state->list_length = -1;

			// get the list data to unnest
			ExpressionExecutor executor;
			vector<TypeId> list_data_types;
			for (auto &exp : select_list) {
				assert(exp->type == ExpressionType::BOUND_UNNEST);
				auto bue = (BoundUnnestExpression *)exp.get();
				list_data_types.push_back(bue->child->return_type);
				executor.AddExpression(*bue->child.get());
			}
			state->list_data.Destroy();
			state->list_data.Initialize(list_data_types);
			executor.Execute(state->child_chunk, state->list_data);

			// paranoia aplenty
			state->child_chunk.Verify();
			state->list_data.Verify();
			assert(state->child_chunk.size() == state->list_data.size());
			assert(state->list_data.column_count() == select_list.size());
		}

		// need to figure out how many times we need to repeat for current row
		if (state->list_length < 0) {
			for (idx_t col_idx = 0; col_idx < state->list_data.column_count(); col_idx++) {
				auto &v = state->list_data.data[col_idx];

				assert(v.type == TypeId::LIST);
				// TODO deal with NULL values here!

				auto list_entry = ((list_entry_t *)v.GetData())[state->parent_position];
				if ((int64_t)list_entry.length > state->list_length) {
					state->list_length = list_entry.length;
				}
			}
		}

		assert(state->list_length >= 0);

		auto this_chunk_len = min((idx_t)STANDARD_VECTOR_SIZE, state->list_length - state->list_position);

		// first cols are from child, last n cols from unnest
		chunk.SetCardinality(this_chunk_len);

		for (idx_t col_idx = 0; col_idx < state->child_chunk.column_count(); col_idx++) {
			// TODO in the future, use a dictionary for this?
			chunk.data[col_idx].Slice(state->child_chunk.data[col_idx], state->parent_position);
			chunk.data[col_idx].vector_type = VectorType::CONSTANT_VECTOR;
		}

		// TODO have fast path for list length = 1
		for (idx_t col_idx = 0; col_idx < state->list_data.column_count(); col_idx++) {

			auto target_col = col_idx + state->child_chunk.column_count();
			// set everything to NULL first
			chunk.data[target_col].nullmask.reset().flip();
			auto &v = state->list_data.data[col_idx];
			auto list_entry = ((list_entry_t *)v.GetData())[state->parent_position];
			auto &child_cc = v.GetListEntry();

			auto this_list_len = min((idx_t)this_chunk_len, list_entry.length - state->list_position);
			auto this_list_start = list_entry.offset + state->list_position;
			if (this_list_len > 0) {
				auto ci_1 = child_cc.LocateChunk(this_list_start);
				auto ci_2 = child_cc.LocateChunk(this_list_start + this_chunk_len - 1);
				auto offset_1 = (idx_t)(this_list_start % STANDARD_VECTOR_SIZE);
				auto &source_1 = child_cc.chunks[ci_1]->data[0];
				// TODO this could be more elegant with a ternary operator
				VectorOperations::Copy(source_1, chunk.data[target_col], offset_1);
				if (ci_1 != ci_2) {
					auto &source_2 = child_cc.chunks[ci_2]->data[0];
					auto n_copied_1 = chunk.data[target_col].size() - (source_1.size() - offset_1);
					// TODO: this is a bit hacky but well
					FlatVector append_dummy;
					append_dummy.Reference(source_2);
					append_dummy.SetCount(this_list_len - n_copied_1);
					VectorOperations::Append(append_dummy, chunk.data[target_col], n_copied_1);
				}
			}
		}

		state->list_position += this_chunk_len;
		if ((int64_t)state->list_position == state->list_length) {
			state->parent_position++;
			state->list_length = -1;
			state->list_position = 0;
		}

		chunk.Verify();
		if (chunk.count > 0) {
			return;
		}
	}
}

unique_ptr<PhysicalOperatorState> PhysicalUnnest::GetOperatorState() {
	return make_unique<PhysicalUnnestOperatorState>(children[0].get());
}
