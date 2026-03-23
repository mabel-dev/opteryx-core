# Function Cost Estimation Tools - Master Index

Complete toolkit for estimating, analyzing, and managing function execution costs in the opteryx function catalog.

## 🚀 Quick Links

- **Getting Started**: [COST_ESTIMATION_QUICKSTART.md](COST_ESTIMATION_QUICKSTART.md) ⭐ Start here!
- **Full Documentation**: [COST_ESTIMATION_README.md](COST_ESTIMATION_README.md)
- **Tool Overview**: [COST_ESTIMATION_SUMMARY.md](COST_ESTIMATION_SUMMARY.md)

## 📋 The Four Tools

### Core Tools

| Tool | Purpose | Command |
|------|---------|---------|
| **estimate_function_costs.py** | Benchmark functions | `python estimate_function_costs.py --functions FUNC1,FUNC2` |
| **import_function_costs.py** | Update catalog | `python import_function_costs.py costs.json --apply` |
| **analyze_function_costs.py** | Generate reports | `python analyze_function_costs.py costs.json` |
| **compare_function_costs.py** | Track changes | `python compare_function_costs.py --catalog --current new.json` |

## ⚡ 5-Minute Start

```bash
# 1. Benchmark a couple functions
python estimate_function_costs.py --functions UPPER,LOWER --output costs.json

# 2. See what changed
python import_function_costs.py costs.json

# 3. Apply if happy
python import_function_costs.py costs.json --apply

# 4. Review results
python analyze_function_costs.py costs.json
```

## 📚 Documentation Map

### For Different Needs

**I want to...**
- 🏃 Get started quickly → [QUICKSTART](COST_ESTIMATION_QUICKSTART.md)
- 📖 Understand everything → [README](COST_ESTIMATION_README.md)
- 🔍 Know what tools exist → [SUMMARY](COST_ESTIMATION_SUMMARY.md)
- 📍 Find what I'm looking for → [This file]

### By Document

| Document | Purpose | Audience |
|----------|---------|----------|
| QUICKSTART.md | 5-min introduction with examples | Everyone |
| README.md | Complete reference | Deep divers |
| SUMMARY.md | Tool capabilities overview | Quick reference |
| This file | Navigation hub | Need to find something |

## 🎯 Common Tasks

### Task: Benchmark specific functions
```bash
python estimate_function_costs.py --functions UPPER,CONCAT,SUBSTRING --output costs.json
```
→ See: [QUICKSTART - Benchmark](COST_ESTIMATION_QUICKSTART.md#11-estimatefunctioncostspy---measure-function-performance)

### Task: Set up initial catalog
```bash
python estimate_function_costs.py --output costs.json
python import_function_costs.py costs.json --apply
```
→ See: [QUICKSTART - Scenario 1](COST_ESTIMATION_QUICKSTART.md#scenario-1-initial-cost-catalog-setup)

### Task: Check for regressions
```bash
python compare_function_costs.py --catalog --current new_costs.json
```
→ See: [QUICKSTART - Scenario 3](COST_ESTIMATION_QUICKSTART.md#scenario-3-investigating-performance-issues)

### Task: Understand cost values
→ See: [README - Cost Per Million Rows](COST_ESTIMATION_README.md#cost-per-million-rows)

### Task: Debug benchmarking issues
→ See: [README - Common Issues](COST_ESTIMATION_README.md#common-issues)

## 🔧 Tool Capabilities Matrix

| Feature | estimate | import | analyze | compare |
|---------|----------|--------|---------|---------|
| Benchmark functions | ✅ | - | - | - |
| Preview changes | - | ✅ | - | - |
| Update source code | - | ✅ | - | - |
| Generate reports | - | - | ✅ | - |
| Compare measurements | - | - | - | ✅ |
| JSON output | ✅ | - | ✅ | ✅ |
| Patch generation | - | ✅ | - | - |
| Dry-run mode | - | ✅ | - | - |

## 📊 Data Flow

```
estimate_function_costs.py
         ↓
    costs.json
    ↙        ↘
import_function_costs.py    analyze_function_costs.py
         ↓                            ↓
   [Source Updated]          [Report Generated]
         ↓
      git commit
         ↓
    [Catalog Updated]

compare_function_costs.py
    ↙                    ↘
baseline.json ---- current.json
    ↓
[Comparison Report]
```

## 🚨 Important Workflows

### Safe Update Workflow ✅
1. Run `estimate_function_costs.py` → generates `costs.json`
2. Run `import_function_costs.py costs.json` → shows preview
3. **REVIEW** the changes
4. Run `import_function_costs.py costs.json --apply` → updates files
5. Run `git diff` to verify
6. Run `git commit` to save

### Risky Workflow ❌ (Don't do this)
- Using `--apply` without previewing first
- Not reviewing the diff before committing
- Assuming all functions benchmarked successfully

## 💡 Pro Tips

- **Start small**: Benchmark 2-3 functions first to understand the tools
- **Preview always**: Never use `--apply` without previewing first
- **High accuracy**: Use `--runs 10` for more reliable estimates
- **Custom sizes**: Use `--sample-sizes` to test specific scales
- **Analyze first**: Run `analyze_function_costs.py` before updating catalog
- **Track changes**: Use `compare_function_costs.py` to catch regressions

## 🆘 Troubleshooting

**Benchmarking takes forever**
→ Use smaller functions or sample sizes: [QUICKSTART - Issues](COST_ESTIMATION_QUICKSTART.md#common-issues--solutions)

**Some functions failed**
→ This is expected; see: [README - Common Issues](COST_ESTIMATION_README.md#some-functions-fail-to-benchmark)

**Need to undo changes**
→ Use git: `git checkout -- opteryx/expression/functions/`

**Want more details**
→ Read the full README: [COST_ESTIMATION_README.md](COST_ESTIMATION_README.md)

## 📖 Full Documentation

- **[COST_ESTIMATION_QUICKSTART.md](COST_ESTIMATION_QUICKSTART.md)** - Start here! 5-min intro with examples
- **[COST_ESTIMATION_README.md](COST_ESTIMATION_README.md)** - Complete reference guide
- **[COST_ESTIMATION_SUMMARY.md](COST_ESTIMATION_SUMMARY.md)** - Tool overview and capabilities

## 🔗 Related Resources

- Function catalog: `opteryx/expression/functions/catalog.py`
- Benchmark model: `opteryx/planner/optimizer/bench/cost_model.py`
- Function implementations: `opteryx/expression/functions/implementations/`
- Statistics system: See memory files for statistical capabilities

## 📝 Version Info

- **Created**: 2026-03-23
- **Tools**: 4 scripts + documentation
- **Status**: Production ready
- **Tested**: ✅ Benchmarking, importing, analysis, comparison

## 🎓 Learning Path

1. **Level 1 - Basics**: [QUICKSTART](COST_ESTIMATION_QUICKSTART.md) (5 min)
2. **Level 2 - Practical**: Run the 5-minute example (5 min)
3. **Level 3 - Advanced**: [Full README](COST_ESTIMATION_README.md) (15 min)
4. **Level 4 - Mastery**: Integrate into your workflow

## 💬 Questions?

- **How do I..?** → Check [QUICKSTART](COST_ESTIMATION_QUICKSTART.md#tldr---get-running-in-5-minutes)
- **What does X do?** → Check [SUMMARY](COST_ESTIMATION_SUMMARY.md)
- **Tell me everything** → Read [README](COST_ESTIMATION_README.md)
- **How do costs work?** → See [README - Cost Per Million Rows](COST_ESTIMATION_README.md#cost-per-million-rows)

---

**Start here**: [COST_ESTIMATION_QUICKSTART.md](COST_ESTIMATION_QUICKSTART.md) ⭐
