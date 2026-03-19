use crate::ast::{Instruction, Program};
use crate::parser::parse_instruction_line;

pub fn apply_patch_text(program: &Program, patch_source: &str) -> Result<Program, String> {
    let mut lines = patch_source
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let header = lines.next().ok_or_else(|| "empty patch".to_string())?;
    if !header.starts_with("patch ") {
        return Err("patch must start with `patch <module>`".to_string());
    }
    let replace = lines
        .next()
        .ok_or_else(|| "patch is missing replace header".to_string())?;
    let replace = replace
        .strip_prefix("replace ")
        .ok_or_else(|| "patch currently supports only replace".to_string())?;
    let mut func_name = None;
    let mut block_name = None;
    let mut instr_name = None;
    let mut saw_with = false;
    for part in replace.split_whitespace() {
        if part == "with" {
            saw_with = true;
            break;
        }
        if let Some(value) = part.strip_prefix("func=") {
            func_name = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("block=") {
            block_name = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("instr=") {
            instr_name = Some(value.to_string());
        }
    }
    if !saw_with {
        return Err("replace header must end with `with`".to_string());
    }
    let new_instruction_line = lines
        .next()
        .ok_or_else(|| "patch is missing replacement instruction".to_string())?;
    let end = lines
        .next()
        .ok_or_else(|| "patch is missing end".to_string())?;
    if end != "end" {
        return Err("patch must end with `end`".to_string());
    }
    let replacement = parse_instruction_line(0, new_instruction_line)?;
    replace_instruction(
        program,
        func_name
            .ok_or_else(|| "patch missing func".to_string())?
            .as_str(),
        block_name
            .ok_or_else(|| "patch missing block".to_string())?
            .as_str(),
        instr_name
            .ok_or_else(|| "patch missing instr".to_string())?
            .as_str(),
        replacement,
    )
}

pub fn replace_instruction(
    program: &Program,
    function_name: &str,
    block_name: &str,
    instruction_name: &str,
    replacement: Instruction,
) -> Result<Program, String> {
    let mut updated = program.clone();
    let mut replaced = false;
    for function in &mut updated.functions {
        if function.name != function_name {
            continue;
        }
        for block in &mut function.blocks {
            if block.label != block_name {
                continue;
            }
            for instruction in &mut block.instructions {
                if instruction.bind == instruction_name {
                    *instruction = replacement.clone();
                    replaced = true;
                }
            }
        }
    }
    if !replaced {
        return Err(format!(
            "instruction {} not found at {}/{}",
            instruction_name, function_name, block_name
        ));
    }
    Ok(updated)
}
