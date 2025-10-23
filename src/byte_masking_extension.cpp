#define DUCKDB_EXTENSION_MAIN

#include "byte_masking_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <sstream>
#include <vector>
#include <regex>
#include <iomanip>

namespace duckdb {

struct MaskEntry {
	string name;
	int start_byte;
	int end_byte;
};

vector<MaskEntry> ParseMaskString(const string &mask_str) {
	vector<MaskEntry> entries;
	
	stringstream ss(mask_str);
	string token;
	
	while (getline(ss, token, ',')) {
		token.erase(0, token.find_first_not_of(" \t"));
		token.erase(token.find_last_not_of(" \t") + 1);
		
		size_t colon_pos = token.find(':');
		if (colon_pos == string::npos) {
			throw InvalidInputException("Invalid mask format. Expected 'name:start-end', got: " + token);
		}
		
		string name = token.substr(0, colon_pos);
		string range_str = token.substr(colon_pos + 1);
		
		// Trim
		name.erase(0, name.find_first_not_of(" \t"));
		name.erase(name.find_last_not_of(" \t") + 1);
		range_str.erase(0, range_str.find_first_not_of(" \t"));
		range_str.erase(range_str.find_last_not_of(" \t") + 1);
		
		size_t dash_pos = range_str.find('-');
		if (dash_pos == string::npos) {
			throw InvalidInputException("Invalid range format. Expected 'start-end', got: " + range_str);
		}
		
		try {
			string start_str = range_str.substr(0, dash_pos);
			string end_str = range_str.substr(dash_pos + 1);
			
			// Trim whitespace
			start_str.erase(0, start_str.find_first_not_of(" \t"));
			start_str.erase(start_str.find_last_not_of(" \t") + 1);
			end_str.erase(0, end_str.find_first_not_of(" \t"));
			end_str.erase(end_str.find_last_not_of(" \t") + 1);
			
			int start_byte = stoi(start_str);
			int end_byte = stoi(end_str);
			
			if (start_byte < 0 || end_byte < start_byte) {
				throw InvalidInputException("Invalid byte range: " + to_string(start_byte) + "-" + to_string(end_byte));
			}
			
			entries.push_back({name, start_byte, end_byte});
		} catch (const std::invalid_argument& e) {
			throw InvalidInputException("Invalid byte range numbers in: " + range_str);
		}
	}
	
	return entries;
}

LogicalType CreateMaskStructType(const vector<MaskEntry> &entries) {
	child_list_t<LogicalType> struct_children;
	for (const auto& entry : entries) {
		struct_children.push_back(make_pair(entry.name, LogicalType::VARCHAR));
	}
	return LogicalType::STRUCT(struct_children);
}

int64_t ExtractBytesToInt(const string &payload, int start_byte, int end_byte, bool big_endian = true) {
	int64_t result = 0;
	int byte_count = 0;
	
	for (int i = start_byte; i <= end_byte && i < (int)payload.length() && byte_count < 8; i++) {
		unsigned char byte = (unsigned char)payload[i];
		
		if (big_endian) {
			// Big-endian: LSB first
			result = (result << 8) | byte;
		} else {
			// Little-endian: MSB first
			result |= ((int64_t)byte) << (byte_count * 8);
		}
		byte_count++;
	}
	
	return result;
}

inline void MaskBytesScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &mask_vector = args.data[0];
	auto &payload_vector = args.data[1];
	
	bool has_endianness_param = args.data.size() > 2;
	bool big_endian = true; // default
	
	UnifiedVectorFormat mask_format, payload_format, endian_format;
	mask_vector.ToUnifiedFormat(args.size(), mask_format);
	payload_vector.ToUnifiedFormat(args.size(), payload_format);
	
	auto mask_data = (string_t *)mask_format.data;
	auto payload_data = (string_t *)payload_format.data;
	string_t *endian_data = nullptr;
	
	if (has_endianness_param) {
		auto &endian_vector = args.data[2];
		endian_vector.ToUnifiedFormat(args.size(), endian_format);
		endian_data = (string_t *)endian_format.data;
	}
	
	auto &key_vector = MapVector::GetKeys(result);
	auto &value_vector = MapVector::GetValues(result);
	auto map_entries = FlatVector::GetData<list_entry_t>(result);
	
	idx_t total_entries = 0;
	
	for (idx_t i = 0; i < args.size(); i++) {
		auto mask_idx = mask_format.sel->get_index(i);
		auto payload_idx = payload_format.sel->get_index(i);
		
		if (!mask_format.validity.RowIsValid(mask_idx) || !payload_format.validity.RowIsValid(payload_idx)) {
			FlatVector::SetNull(result, i, true);
			map_entries[i].offset = total_entries;
			map_entries[i].length = 0;
			continue;
		}
		
		try {
			string mask = mask_data[mask_idx].GetString();
			string_t blob_data = payload_data[payload_idx];
			string payload(blob_data.GetDataUnsafe(), blob_data.GetSize());
			
			// Get endianness for this row
			bool current_big_endian = big_endian;
			if (has_endianness_param) {
				auto endian_idx = endian_format.sel->get_index(i);
				if (endian_format.validity.RowIsValid(endian_idx)) {
					string endian_str = endian_data[endian_idx].GetString();
					if (endian_str == "big") {
						current_big_endian = true;
					} else if (endian_str == "little") {
						current_big_endian = false;
					} else {
						throw InvalidInputException("Invalid endianness parameter. Must be 'big' or 'little', got: " + endian_str);
					}
				}
			}
			
			vector<MaskEntry> entries = ParseMaskString(mask);
			
			map_entries[i].offset = total_entries;
			map_entries[i].length = entries.size();
			
			if (total_entries + entries.size() > STANDARD_VECTOR_SIZE) {
				throw InvalidInputException("Exceeded maximum number of map entries in mask_bytes function");
			}
			
			auto key_data = FlatVector::GetData<string_t>(key_vector);
			auto value_data = FlatVector::GetData<int64_t>(value_vector);
			
			for (size_t j = 0; j < entries.size(); j++) {
				const auto& entry = entries[j];
				
				key_data[total_entries + j] = StringVector::AddString(key_vector, entry.name);
				
				int64_t int_value = ExtractBytesToInt(payload, entry.start_byte, entry.end_byte, current_big_endian);
				value_data[total_entries + j] = int_value;
			}
			
			total_entries += entries.size();
			
		} catch (const Exception& e) {
			throw e;
		} catch (const std::exception& e) {
			throw InvalidInputException("Error processing mask_bytes: " + string(e.what()));
		}
	}
	
	ListVector::SetListSize(result, total_entries);
}


static void LoadInternal(ExtensionLoader &loader) {
	// Register mask_bytes function that returns a MAP<VARCHAR, BIGINT>
	// Version with endianness parameter (accepts 'big' or 'little')
	auto mask_bytes_function_full = ScalarFunction("mask_bytes", {LogicalType::VARCHAR, LogicalType::BLOB, LogicalType::VARCHAR}, 
	                                              LogicalType::MAP(LogicalType::VARCHAR, LogicalType::BIGINT), MaskBytesScalarFun);
	loader.RegisterFunction(mask_bytes_function_full);
	
	// Version without endianness parameter (defaults to little-endian)
	auto mask_bytes_function_simple = ScalarFunction("mask_bytes", {LogicalType::VARCHAR, LogicalType::BLOB}, 
	                                                 LogicalType::MAP(LogicalType::VARCHAR, LogicalType::BIGINT), MaskBytesScalarFun);
	loader.RegisterFunction(mask_bytes_function_simple);
}

void ByteMaskingExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string ByteMaskingExtension::Name() {
	return "byte_masking";
}

std::string ByteMaskingExtension::Version() const {
#ifdef EXT_VERSION_BYTE_MASKING
	return EXT_VERSION_BYTE_MASKING;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(byte_masking, loader) {
	duckdb::LoadInternal(loader);
}
}
