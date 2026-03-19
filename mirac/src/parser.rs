use crate::ast::{
    Block, ConstDecl, EnumVariant, Field, Function, Instruction, Program, Target, Terminator,
    TestCase, TypeDecl, TypeDeclBody,
};
use crate::types::{split_top_level, split_top_level_whitespace, TypeRef};

#[derive(Debug, Clone)]
struct Line {
    number: usize,
    text: String,
}

pub fn parse_program(source: &str) -> Result<Program, String> {
    let lines: Vec<Line> = source
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(Line {
                    number: index + 1,
                    text: trimmed.to_string(),
                })
            }
        })
        .collect();
    if lines.is_empty() {
        return Err("empty MIRA source".to_string());
    }
    let first = &lines[0];
    let module = first
        .text
        .strip_prefix("module ")
        .ok_or_else(|| format!("line {}: program must start with module", first.number))?
        .trim()
        .to_string();

    let mut target = None;
    let mut uses = Vec::new();
    let mut types = Vec::new();
    let mut consts = Vec::new();
    let mut functions = Vec::new();
    let mut index = 1usize;
    while index < lines.len() {
        let line = &lines[index];
        if let Some(rest) = line.text.strip_prefix("target ") {
            target = Some(rest.trim().to_string());
            index += 1;
            continue;
        }
        if let Some(rest) = line.text.strip_prefix("use ") {
            uses.push(rest.trim().to_string());
            index += 1;
            continue;
        }
        if let Some(rest) = line.text.strip_prefix("type ") {
            types.push(parse_type_decl(line.number, rest)?);
            index += 1;
            continue;
        }
        if let Some(rest) = line.text.strip_prefix("const ") {
            consts.push(parse_const(line.number, rest)?);
            index += 1;
            continue;
        }
        if line.text.starts_with("func ") {
            let (function, next) = parse_function(&lines, index)?;
            functions.push(function);
            index = next;
            continue;
        }
        return Err(format!(
            "line {}: unexpected top-level form {}",
            line.number, line.text
        ));
    }

    Ok(Program {
        module,
        target,
        uses,
        types,
        consts,
        functions,
    })
}

fn parse_function(lines: &[Line], start: usize) -> Result<(Function, usize), String> {
    let name = lines[start]
        .text
        .strip_prefix("func ")
        .ok_or_else(|| format!("line {}: invalid function header", lines[start].number))?
        .trim()
        .to_string();
    let mut args = Vec::new();
    let mut ret = None;
    let mut effects = Vec::new();
    let mut capabilities = Vec::new();
    let mut specs = Vec::new();
    let mut blocks = Vec::new();
    let mut tests = Vec::new();
    let mut index = start + 1;

    while index < lines.len() {
        let line = &lines[index];
        if let Some(rest) = line.text.strip_prefix("arg ") {
            args.push(parse_field(line.number, rest)?);
            index += 1;
            continue;
        }
        if let Some(rest) = line.text.strip_prefix("ret ") {
            ret = Some(TypeRef::parse(rest.trim())?);
            index += 1;
            continue;
        }
        if let Some(rest) = line.text.strip_prefix("eff ") {
            effects = rest
                .split_whitespace()
                .map(|item| item.to_string())
                .collect();
            index += 1;
            continue;
        }
        if let Some(rest) = line.text.strip_prefix("cap ") {
            capabilities.push(rest.trim().to_string());
            index += 1;
            continue;
        }
        if let Some(rest) = line.text.strip_prefix("spec ") {
            specs.push(rest.trim().to_string());
            index += 1;
            continue;
        }
        if line.text.starts_with("block ") {
            let (block, next) = parse_block(lines, index)?;
            blocks.push(block);
            index = next;
            continue;
        }
        if line.text.starts_with("test ") {
            let (test, next) = parse_test(lines, index)?;
            tests.push(test);
            index = next;
            continue;
        }
        if line.text == "end" {
            let ret =
                ret.ok_or_else(|| format!("line {}: function {name} is missing ret", line.number))?;
            return Ok((
                Function {
                    name,
                    args,
                    ret,
                    effects,
                    capabilities,
                    specs,
                    blocks,
                    tests,
                },
                index + 1,
            ));
        }
        return Err(format!(
            "line {}: unexpected function form {}",
            line.number, line.text
        ));
    }
    Err(format!("function {name} is missing end"))
}

fn parse_block(lines: &[Line], start: usize) -> Result<(Block, usize), String> {
    let line = &lines[start];
    let header = line
        .text
        .strip_prefix("block ")
        .ok_or_else(|| format!("line {}: invalid block header", line.number))?
        .trim();
    let (label, params) = if let Some((label, rest)) = header.split_once('(') {
        let params_text = rest
            .strip_suffix(')')
            .ok_or_else(|| format!("line {}: invalid block params", line.number))?;
        let params = split_top_level(params_text, ',')
            .iter()
            .map(|part| parse_field(line.number, part))
            .collect::<Result<Vec<_>, _>>()?;
        (label.trim().to_string(), params)
    } else {
        (header.to_string(), Vec::new())
    };

    let mut instructions = Vec::new();
    let mut terminator = None;
    let mut index = start + 1;
    while index < lines.len() {
        let line = &lines[index];
        if line.text.starts_with("block ") || line.text.starts_with("test ") || line.text == "end" {
            break;
        }
        if terminator.is_some() {
            return Err(format!(
                "line {}: block {label} contains code after terminator",
                line.number
            ));
        }
        if let Some(rest) = line.text.strip_prefix("return ") {
            terminator = Some(Terminator::Return(rest.trim().to_string()));
            index += 1;
            continue;
        }
        if let Some(rest) = line.text.strip_prefix("jump ") {
            terminator = Some(Terminator::Jump(parse_target(line.number, rest)?));
            index += 1;
            continue;
        }
        if let Some(rest) = line.text.strip_prefix("branch ") {
            let (condition, targets_text) = rest
                .split_once(char::is_whitespace)
                .ok_or_else(|| format!("line {}: invalid branch terminator", line.number))?;
            let targets = split_top_level_whitespace(targets_text);
            if targets.len() != 2 {
                return Err(format!("line {}: branch expects two targets", line.number));
            }
            terminator = Some(Terminator::Branch {
                condition: condition.trim().to_string(),
                truthy: parse_target(line.number, &targets[0])?,
                falsy: parse_target(line.number, &targets[1])?,
            });
            index += 1;
            continue;
        }
        if let Some(rest) = line.text.strip_prefix("match ") {
            let (value, arms_text) = rest
                .split_once(char::is_whitespace)
                .ok_or_else(|| format!("line {}: invalid match terminator", line.number))?;
            let arms = split_top_level_whitespace(arms_text)
                .into_iter()
                .map(|part| parse_target(line.number, &part))
                .collect::<Result<Vec<_>, _>>()?;
            if arms.is_empty() {
                return Err(format!(
                    "line {}: match expects at least one target arm",
                    line.number
                ));
            }
            terminator = Some(Terminator::Match {
                value: value.trim().to_string(),
                arms,
            });
            index += 1;
            continue;
        }
        instructions.push(parse_instruction(line.number, &line.text)?);
        index += 1;
    }
    let terminator = terminator
        .ok_or_else(|| format!("line {}: block {label} is missing terminator", line.number))?;
    Ok((
        Block {
            label,
            params,
            instructions,
            terminator,
        },
        index,
    ))
}

fn parse_test(lines: &[Line], start: usize) -> Result<(TestCase, usize), String> {
    let name = lines[start]
        .text
        .strip_prefix("test ")
        .ok_or_else(|| format!("line {}: invalid test header", lines[start].number))?
        .trim()
        .to_string();
    let mut call = None;
    let mut inputs = Vec::new();
    let mut expected = None;
    let mut index = start + 1;
    while index < lines.len() {
        let line = &lines[index];
        if line.text.starts_with("block ") || line.text.starts_with("test ") || line.text == "end" {
            break;
        }
        if let Some(rest) = line.text.strip_prefix("call ") {
            call = Some(rest.trim().to_string());
        } else if let Some(rest) = line.text.strip_prefix("in ") {
            let (name, value) = rest
                .split_once('=')
                .ok_or_else(|| format!("line {}: test input must be name=value", line.number))?;
            inputs.push((name.trim().to_string(), value.trim().to_string()));
        } else if let Some(rest) = line.text.strip_prefix("out ") {
            expected = Some(rest.trim().to_string());
        } else {
            return Err(format!(
                "line {}: unexpected test form {}",
                line.number, line.text
            ));
        }
        index += 1;
    }
    let expected = expected.ok_or_else(|| format!("test {name} is missing out"))?;
    Ok((
        TestCase {
            name,
            call,
            inputs,
            expected,
        },
        index,
    ))
}

fn parse_field(line_number: usize, text: &str) -> Result<Field, String> {
    let (name, ty) = text
        .split_once(':')
        .ok_or_else(|| format!("line {line_number}: invalid field {text}"))?;
    Ok(Field {
        name: name.trim().to_string(),
        ty: TypeRef::parse(ty.trim())?,
    })
}

fn parse_const(line_number: usize, text: &str) -> Result<ConstDecl, String> {
    let (lhs, value) = text
        .split_once('=')
        .ok_or_else(|| format!("line {line_number}: invalid const declaration {text}"))?;
    let field = parse_field(line_number, lhs.trim())?;
    Ok(ConstDecl {
        name: field.name,
        ty: field.ty,
        value: value.trim().to_string(),
    })
}

fn parse_type_decl(line_number: usize, text: &str) -> Result<TypeDecl, String> {
    let (name, body) = text
        .split_once('=')
        .ok_or_else(|| format!("line {line_number}: invalid type declaration {text}"))?;
    let name = name.trim().to_string();
    let body = body.trim();
    if let Some(fields_text) = body
        .strip_prefix("struct[")
        .and_then(|rest| rest.strip_suffix(']'))
    {
        let fields = if fields_text.trim().is_empty() {
            Vec::new()
        } else {
            split_top_level(fields_text, ',')
                .iter()
                .map(|part| parse_field(line_number, part))
                .collect::<Result<Vec<_>, _>>()?
        };
        return Ok(TypeDecl {
            name,
            body: TypeDeclBody::Struct { fields },
        });
    }
    if let Some(variants_text) = body
        .strip_prefix("enum[")
        .and_then(|rest| rest.strip_suffix(']'))
    {
        let variants = if variants_text.trim().is_empty() {
            Vec::new()
        } else {
            split_top_level(variants_text, ',')
                .into_iter()
                .map(|item| parse_enum_variant(line_number, &item))
                .collect::<Result<Vec<_>, _>>()?
        };
        return Ok(TypeDecl {
            name,
            body: TypeDeclBody::Enum { variants },
        });
    }
    Err(format!(
        "line {line_number}: unsupported type declaration body {body}"
    ))
}

pub fn parse_instruction_line(line_number: usize, text: &str) -> Result<Instruction, String> {
    let (lhs, rhs) = text
        .split_once('=')
        .ok_or_else(|| format!("line {line_number}: invalid instruction {text}"))?;
    let field = parse_field(line_number, lhs.trim())?;
    let mut rhs_parts = rhs.trim().split_whitespace();
    let op = rhs_parts
        .next()
        .ok_or_else(|| format!("line {line_number}: missing instruction op"))?
        .to_string();
    let args = rhs_parts.map(|part| part.to_string()).collect();
    Ok(Instruction {
        bind: field.name,
        ty: field.ty,
        op,
        args,
    })
}

fn parse_instruction(line_number: usize, text: &str) -> Result<Instruction, String> {
    parse_instruction_line(line_number, text)
}

fn parse_enum_variant(line_number: usize, text: &str) -> Result<EnumVariant, String> {
    let text = text.trim();
    if text.is_empty() {
        return Err(format!("line {line_number}: enum variant cannot be empty"));
    }
    if let Some((name, rest)) = text.split_once('[') {
        let fields_text = rest
            .strip_suffix(']')
            .ok_or_else(|| format!("line {line_number}: invalid enum variant {text}"))?;
        let fields = if fields_text.trim().is_empty() {
            Vec::new()
        } else {
            split_top_level(fields_text, ',')
                .iter()
                .map(|part| parse_field(line_number, part))
                .collect::<Result<Vec<_>, _>>()?
        };
        return Ok(EnumVariant {
            name: name.trim().to_string(),
            fields,
        });
    }
    Ok(EnumVariant {
        name: text.to_string(),
        fields: Vec::new(),
    })
}

fn parse_target(line_number: usize, text: &str) -> Result<Target, String> {
    let text = text.trim();
    if let Some((label, rest)) = text.split_once('(') {
        let args_text = rest
            .strip_suffix(')')
            .ok_or_else(|| format!("line {line_number}: invalid target {text}"))?;
        let args = split_top_level(args_text, ',');
        return Ok(Target {
            label: label.trim().to_string(),
            args,
        });
    }
    Ok(Target {
        label: text.to_string(),
        args: Vec::new(),
    })
}
