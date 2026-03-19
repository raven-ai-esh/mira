use serde::{Deserialize, Serialize};

use crate::ast::Program;
use crate::codegen_c::{lower_program, LoweredProgram};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinaryArtifact {
    LegacyProgram(Program),
    LoweredProgram(LoweredProgram),
}

pub fn encode_program(program: &Program) -> Result<Vec<u8>, String> {
    encode_lowered_program(&lower_program(program)?)
}

pub fn encode_lowered_program(program: &LoweredProgram) -> Result<Vec<u8>, String> {
    let mut out = b"MIRB3".to_vec();
    let payload = rmp_serde::to_vec_named(program)
        .map_err(|error| format!("failed to serialize lowered binary IR payload: {error}"))?;
    out.extend_from_slice(&payload);
    Ok(out)
}

pub fn decode_artifact(bytes: &[u8]) -> Result<BinaryArtifact, String> {
    if bytes.len() < 5 {
        return Err("invalid MIRB header".to_string());
    }
    match &bytes[..5] {
        b"MIRB3" => rmp_serde::from_slice(&bytes[5..])
            .map(BinaryArtifact::LoweredProgram)
            .map_err(|error| format!("failed to decode lowered binary IR: {error}")),
        b"MIRB2" => rmp_serde::from_slice(&bytes[5..])
            .map(BinaryArtifact::LegacyProgram)
            .map_err(|error| format!("failed to decode legacy binary IR: {error}")),
        b"MIRB1" => serde_json::from_slice(&bytes[5..])
            .map(BinaryArtifact::LegacyProgram)
            .map_err(|error| format!("failed to decode legacy binary IR: {error}")),
        _ => Err("invalid MIRB header".to_string()),
    }
}

#[allow(dead_code)]
pub fn decode_program(bytes: &[u8]) -> Result<Program, String> {
    match decode_artifact(bytes)? {
        BinaryArtifact::LegacyProgram(program) => Ok(program),
        BinaryArtifact::LoweredProgram(_) => Err(
            "MIRB3 stores lowered backend IR and cannot be decoded as an AST Program".to_string(),
        ),
    }
}
