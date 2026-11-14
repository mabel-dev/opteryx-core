#pragma once
// The one duplicate-column-name refusal shared by rugo's writers and readers.
// Columns are resolved by name, so a second column of the same name is
// unreachable (first match wins; JSONL objects collapse the keys outright) —
// writers refuse to produce one and readers refuse a file that holds one.

#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace rugo {

// `name_at(i)` returns column i's name (as a std::string const&) for i < n.
// Throws std::invalid_argument naming every repeated name, in first-repeat order.
template <class NameAt>
inline void require_unique_column_names(const char *who, size_t n, NameAt name_at) {
  std::unordered_set<std::string_view> seen;
  std::unordered_set<std::string_view> reported;
  std::vector<std::string_view> dups;
  seen.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    const std::string &nm = name_at(i);
    if (!seen.insert(nm).second && reported.insert(nm).second)
      dups.push_back(nm);
  }
  if (dups.empty())
    return;
  std::string msg(who);
  msg += dups.size() == 1 ? ": column name " : ": column names ";
  for (size_t i = 0; i < dups.size(); ++i) {
    if (i) msg += ", ";
    msg += '\'';
    msg.append(dups[i]);
    msg += '\'';
  }
  msg += dups.size() == 1 ? " appears more than once" : " appear more than once";
  throw std::invalid_argument(msg);
}

} // namespace rugo
