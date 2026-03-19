use crate::ast::Program;

pub fn render_program_json(program: &Program) -> Result<String, String> {
    serde_json::to_string_pretty(program)
        .map_err(|error| format!("failed to serialize AST JSON: {error}"))
}

pub fn parse_program_json(source: &str) -> Result<Program, String> {
    serde_json::from_str(source).map_err(|error| format!("failed to parse AST JSON: {error}"))
}

pub fn ast_schema_json() -> &'static str {
    include_str!("../schema/mira_ast_v1.json")
}
