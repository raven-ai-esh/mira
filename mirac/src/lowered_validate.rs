use std::collections::{BTreeSet, HashMap};

use crate::codegen_c::{LoweredFunction, LoweredProgram, LoweredTerminator};

pub fn validate_lowered_program(program: &LoweredProgram) -> Vec<String> {
    let mut diagnostics = Vec::new();
    if program.module.trim().is_empty() {
        diagnostics.push("LOWERED_MODULE_INVALID: module name must not be empty".to_string());
    }

    let mut function_names = BTreeSet::new();
    for function in &program.functions {
        if !function_names.insert(function.name.clone()) {
            diagnostics.push(format!(
                "LOWERED_DUPLICATE_FUNCTION: function {} appears multiple times",
                function.name
            ));
        }
        validate_lowered_function(function, &mut diagnostics);
    }

    let function_map = program
        .functions
        .iter()
        .map(|function| (function.name.as_str(), function))
        .collect::<HashMap<_, _>>();
    for test in &program.tests {
        let Some(function) = function_map.get(test.call.function_name.as_str()) else {
            diagnostics.push(format!(
                "LOWERED_TEST_UNKNOWN_FUNCTION: test {}.{} targets missing function {}",
                test.owner, test.name, test.call.function_name
            ));
            continue;
        };
        if test.call.args.len() != function.args.len() {
            diagnostics.push(format!(
                "LOWERED_TEST_ARITY: test {}.{} passes {} args to {} but function expects {}",
                test.owner,
                test.name,
                test.call.args.len(),
                test.call.function_name,
                function.args.len()
            ));
        }
        if test.call.ret_c_type != function.ret_c_type {
            diagnostics.push(format!(
                "LOWERED_TEST_RETURN_TYPE: test {}.{} expects return type {} but function {} returns {}",
                test.owner, test.name, test.call.ret_c_type, test.call.function_name, function.ret_c_type
            ));
        }
        if test.call.result_name.trim().is_empty() {
            diagnostics.push(format!(
                "LOWERED_TEST_RESULT_NAME: test {}.{} has empty result binding",
                test.owner, test.name
            ));
        }
        if test.assertion.condition.trim().is_empty() {
            diagnostics.push(format!(
                "LOWERED_TEST_ASSERTION: test {}.{} has empty assertion condition",
                test.owner, test.name
            ));
        }
    }

    diagnostics
}

fn validate_lowered_function(function: &LoweredFunction, diagnostics: &mut Vec<String>) {
    let declaration_names = function
        .declarations
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<BTreeSet<_>>();
    if declaration_names.len() != function.declarations.len() {
        diagnostics.push(format!(
            "LOWERED_DUPLICATE_DECLARATION: function {} has duplicate declaration names",
            function.name
        ));
    }

    let block_labels = function
        .blocks
        .iter()
        .map(|block| block.label.as_str())
        .collect::<BTreeSet<_>>();
    if block_labels.len() != function.blocks.len() {
        diagnostics.push(format!(
            "LOWERED_DUPLICATE_BLOCK: function {} has duplicate block labels",
            function.name
        ));
    }
    if !block_labels.contains("b0") {
        diagnostics.push(format!(
            "LOWERED_MISSING_ENTRY: function {} is missing entry block b0",
            function.name
        ));
    }

    let mut has_rand_expr = false;
    for block in &function.blocks {
        for statement in &block.statements {
            match statement {
                crate::codegen_c::LoweredStatement::Assign(assignment) => {
                    if !declaration_names.contains(assignment.target.as_str()) {
                        diagnostics.push(format!(
                            "LOWERED_UNKNOWN_ASSIGN_TARGET: function {} block {} assigns unknown target {}",
                            function.name, block.label, assignment.target
                        ));
                    }
                    if assignment.expr.contains("mira_rand_next_u32") {
                        has_rand_expr = true;
                    }
                }
            }
        }
        validate_lowered_terminator(
            function,
            block.label.as_str(),
            &block.terminator,
            &declaration_names,
            &block_labels,
            diagnostics,
        );
    }

    if function.uses_arena {
        let all_returns_release = function.blocks.iter().all(|block| match &block.terminator {
            LoweredTerminator::Return { release_arena, .. } => *release_arena,
            _ => true,
        });
        if !all_returns_release {
            diagnostics.push(format!(
                "LOWERED_ARENA_RETURN: function {} uses arena but has a return without release",
                function.name
            ));
        }
    } else if function.blocks.iter().any(|block| match &block.terminator {
        LoweredTerminator::Return { release_arena, .. } => *release_arena,
        _ => false,
    }) {
        diagnostics.push(format!(
            "LOWERED_ARENA_FLAG: function {} does not use arena but has an arena-release return",
            function.name
        ));
    }

    if function.rand_seed.is_none() && has_rand_expr {
        diagnostics.push(format!(
            "LOWERED_RAND_STATE: function {} uses rand state but has no rand seed",
            function.name
        ));
    }
}

fn validate_lowered_terminator(
    function: &LoweredFunction,
    block_label: &str,
    terminator: &LoweredTerminator,
    declaration_names: &BTreeSet<&str>,
    block_labels: &BTreeSet<&str>,
    diagnostics: &mut Vec<String>,
) {
    match terminator {
        LoweredTerminator::Return { expr, .. } => {
            if expr.trim().is_empty() {
                diagnostics.push(format!(
                    "LOWERED_EMPTY_RETURN: function {} block {} returns empty expression",
                    function.name, block_label
                ));
            }
        }
        LoweredTerminator::Jump { edge } => {
            validate_edge(
                function,
                block_label,
                edge.label.as_str(),
                &edge.assignments,
                declaration_names,
                block_labels,
                diagnostics,
            );
        }
        LoweredTerminator::Branch {
            condition,
            truthy,
            falsy,
            ..
        } => {
            if condition.trim().is_empty() {
                diagnostics.push(format!(
                    "LOWERED_EMPTY_BRANCH_CONDITION: function {} block {} has empty branch condition",
                    function.name, block_label
                ));
            }
            validate_edge(
                function,
                block_label,
                truthy.label.as_str(),
                &truthy.assignments,
                declaration_names,
                block_labels,
                diagnostics,
            );
            validate_edge(
                function,
                block_label,
                falsy.label.as_str(),
                &falsy.assignments,
                declaration_names,
                block_labels,
                diagnostics,
            );
        }
        LoweredTerminator::Match {
            value,
            cases,
            default,
            ..
        } => {
            if value.trim().is_empty() {
                diagnostics.push(format!(
                    "LOWERED_EMPTY_MATCH_VALUE: function {} block {} has empty match value",
                    function.name, block_label
                ));
            }
            let mut tags = BTreeSet::new();
            for case in cases {
                if !tags.insert(case.tag_index) {
                    diagnostics.push(format!(
                        "LOWERED_DUPLICATE_MATCH_CASE: function {} block {} has duplicate case {}",
                        function.name, block_label, case.tag_index
                    ));
                }
                validate_edge(
                    function,
                    block_label,
                    case.edge.label.as_str(),
                    &case.edge.assignments,
                    declaration_names,
                    block_labels,
                    diagnostics,
                );
            }
            validate_edge(
                function,
                block_label,
                default.label.as_str(),
                &default.assignments,
                declaration_names,
                block_labels,
                diagnostics,
            );
        }
    }
}

fn validate_edge(
    function: &LoweredFunction,
    block_label: &str,
    target_label: &str,
    assignments: &[crate::codegen_c::LoweredAssignment],
    declaration_names: &BTreeSet<&str>,
    block_labels: &BTreeSet<&str>,
    diagnostics: &mut Vec<String>,
) {
    if !block_labels.contains(target_label) {
        diagnostics.push(format!(
            "LOWERED_UNKNOWN_EDGE_TARGET: function {} block {} jumps to missing block {}",
            function.name, block_label, target_label
        ));
    }
    for assignment in assignments {
        if !declaration_names.contains(assignment.target.as_str()) {
            diagnostics.push(format!(
                "LOWERED_UNKNOWN_EDGE_ASSIGNMENT: function {} block {} edge to {} assigns unknown target {}",
                function.name, block_label, target_label, assignment.target
            ));
        }
        if assignment.expr.trim().is_empty() {
            diagnostics.push(format!(
                "LOWERED_EMPTY_EDGE_EXPR: function {} block {} edge to {} has empty assignment for {}",
                function.name, block_label, target_label, assignment.target
            ));
        }
    }
}
