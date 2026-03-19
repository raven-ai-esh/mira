use crate::types::TypeRef;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub ty: TypeRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstDecl {
    pub name: String,
    pub ty: TypeRef,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnumVariant {
    pub name: String,
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum TypeDeclBody {
    Struct { fields: Vec<Field> },
    Enum { variants: Vec<EnumVariant> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeDecl {
    pub name: String,
    pub body: TypeDeclBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Instruction {
    pub bind: String,
    pub ty: TypeRef,
    pub op: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    pub label: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Terminator {
    Return(String),
    Jump(Target),
    Branch {
        condition: String,
        truthy: Target,
        falsy: Target,
    },
    Match {
        value: String,
        arms: Vec<Target>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    pub label: String,
    pub params: Vec<Field>,
    pub instructions: Vec<Instruction>,
    pub terminator: Terminator,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestCase {
    pub name: String,
    pub call: Option<String>,
    pub inputs: Vec<(String, String)>,
    pub expected: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Function {
    pub name: String,
    pub args: Vec<Field>,
    pub ret: TypeRef,
    pub effects: Vec<String>,
    pub capabilities: Vec<String>,
    pub specs: Vec<String>,
    pub blocks: Vec<Block>,
    pub tests: Vec<TestCase>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Program {
    pub module: String,
    pub target: Option<String>,
    pub uses: Vec<String>,
    pub types: Vec<TypeDecl>,
    pub consts: Vec<ConstDecl>,
    pub functions: Vec<Function>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub phase: String,
    pub node: String,
    pub error_code: String,
    pub message: String,
    pub expected: Option<String>,
    pub observed: Option<String>,
    pub fix_hint: Option<String>,
}

impl Diagnostic {
    pub fn new(phase: &str, node: String, error_code: &str, message: impl Into<String>) -> Self {
        Self {
            phase: phase.to_string(),
            node,
            error_code: error_code.to_string(),
            message: message.into(),
            expected: None,
            observed: None,
            fix_hint: None,
        }
    }

    pub fn with_expected(mut self, expected: impl Into<String>) -> Self {
        self.expected = Some(expected.into());
        self
    }

    pub fn with_observed(mut self, observed: impl Into<String>) -> Self {
        self.observed = Some(observed.into());
        self
    }

    pub fn with_fix_hint(mut self, fix_hint: impl Into<String>) -> Self {
        self.fix_hint = Some(fix_hint.into());
        self
    }
}

pub fn node_path(parts: &[String]) -> String {
    parts.join("/")
}
