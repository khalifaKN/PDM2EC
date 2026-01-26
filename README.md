# A stateful, orchestrated ETL-based enterprise integration architecture.


# Employee Creation Order Resolver

## Overview

When creating employees in HR systems SAP EC, **manager relationships must exist before they can be referenced**.

This creates a challenge when onboarding **multiple new employees at once**, because:

* Some employees report to other **new** employees
* Some employees form **chains of dependencies**
* Some employees create **circular dependencies**
* Some employees reference **missing or unknown managers**

The **EmployeeCreationOrderResolver** analyzes these dependencies and determines a **safe creation order** that:

* Minimizes errors
* Allows parallel creation where possible
* Detects and handles problematic cases gracefully


## Problem Statement

Each employee may reference other employees via one or more fields:

* `manager`
* `matrix_manager`
* `hr`

An employee **cannot be created** until all referenced employees already exist.

### Example

Alice → manager: Bob
Bob   → manager: Carol
Carol → manager: (none)

Correct creation order:

Carol → Bob → Alice

## What This Resolver Does

The resolver:

1. Builds a **dependency graph** between new employees
2. Determines which employees can be created immediately
3. Groups employees into **creation batches**
4. Detects **circular dependencies**
5. Detects **missing dependencies**
6. Provides a **summary report** for diagnostics

## Input Data

### New Employees

A `pandas.DataFrame` containing employees to be created.

Required column:

* `userid`

Optional dependency columns:

* `manager`
* `matrix_manager`
* `hr`

Example:

| userid | manager | matrix_manager | hr |
| ------ | ------- | -------------- | -- |
| a      | b       |                |    |
| b      | c       |                |    |
| c      |         |                |    |

---

### Existing Employees

A `pandas.DataFrame` containing employees that already exist in the system.

| userid |
| ------ |
| x      |
| y      |
| z      |

These employees **do not need to be created** and can be safely referenced.


## Dependency Resolution Strategy

### 1. Dependency Graph

Each new employee is represented as a node.

An edge is created when:

Employee A depends on Employee B

Which means:

B → A

### 2. Level-Based Topological Sorting

The resolver uses **Kahn’s Algorithm** to determine the order.

Employees are grouped into **batches**, where:

* All employees in a batch have no unresolved dependencies
* A batch can be processed **in parallel**


## Example: Simple Dependencies

### Input

A → manager: B
B → manager: C
C → no manager

### Output Batches

Batch 1: C
Batch 2: B
Batch 3: A


## Example: Parallel Creation

### Input

A → manager: C
B → manager: C
C → no manager

### Output Batches

Batch 1: C
Batch 2: A, B

Both `A` and `B` can be created **at the same time** after `C`.

## Example: Existing Employees

### Input


A → manager: X
B → manager: X
X → already exists


### Output


Batch 1: A, B


Because `X` already exists, there is **no dependency between A and B**.


## Circular Dependencies

### What Is a Cycle?

A circular dependency occurs when employees reference each other directly or indirectly.

### Example

A → manager: B
B → manager: C
C → manager: A

This creates a cycle:

A → B → C → A

❌ None of these employees can be created first.


### How Cycles Are Handled

When a cycle is detected:

1. All cycle members are grouped into a **final batch**
2. Manager fields referencing other cycle members are **temporarily cleared**
3. Employees are created without those fields
4. Manager relationships can be **updated afterward**

This prevents the entire process from failing.


## Example: Cycle Handling

### Input

A → manager: B
B → manager: C
C → manager: A

### Temporary Resolution

Batch 1:
A (manager cleared)
B (manager cleared)
C (manager cleared)

Managers can be reassigned after creation.

## Missing Dependencies

### What Is a Missing Dependency?

A dependency is considered **missing** when it references:

* A userid not present in new employees
* AND not present in existing employees

### Example

A → manager: Z
Z → does not exist

This is **not blocked**, but it is **reported**.


## Dependency Summary Report

The method `get_dependency_summary()` provides diagnostics.

### Example Output

```python
{
    "total_new_employees": 10,
    "employees_with_no_dependencies": 4,
    "employees_with_dependencies": 6,
    "employees_in_cycles": 3,
    "cycle_userids": ["a", "b", "c"],
    "missing_dependencies": [
        {
            "userid": "d",
            "field": "manager",
            "missing_dependency": "z"
        }
    ],
    "missing_dependency_count": 1
}
```

---

## Design Principles

* Deterministic results
* Parallel execution when possible
* Explicit cycle detection
* Safe failure handling
* Clear logging and diagnostics

## Summary

The **EmployeeCreationOrderResolver** ensures that:

* Employees are created in the correct order
* Dependencies are respected
* Cycles do not block the entire process
* Data issues are clearly reported

It is a **safe and scalable solution** for employee onboarding in enterprise HR systems.
