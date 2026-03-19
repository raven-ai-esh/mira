use crate::ast::{Block, Function, Program, Terminator, TypeDeclBody};

pub fn format_program(program: &Program) -> String {
    let mut out = String::new();
    out.push_str(&format!("module {}\n", program.module));
    if let Some(target) = &program.target {
        out.push_str(&format!("target {}\n", target));
    }
    for usage in &program.uses {
        out.push_str(&format!("use {}\n", usage));
    }
    for item in &program.types {
        out.push_str(&match &item.body {
            TypeDeclBody::Struct { fields } => format!(
                "type {} = struct[{}]\n",
                item.name,
                fields
                    .iter()
                    .map(|field| format!("{}:{}", field.name, field.ty))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            TypeDeclBody::Enum { variants } => format!(
                "type {} = enum[{}]\n",
                item.name,
                variants
                    .iter()
                    .map(|variant| {
                        if variant.fields.is_empty() {
                            variant.name.clone()
                        } else {
                            format!(
                                "{}[{}]",
                                variant.name,
                                variant
                                    .fields
                                    .iter()
                                    .map(|field| format!("{}:{}", field.name, field.ty))
                                    .collect::<Vec<_>>()
                                    .join(",")
                            )
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        });
    }
    for item in &program.consts {
        out.push_str(&format!(
            "const {}:{} = {}\n",
            item.name, item.ty, item.value
        ));
    }
    for function in &program.functions {
        out.push('\n');
        out.push_str(&format_function(function));
    }
    out
}

fn format_function(function: &Function) -> String {
    let mut out = String::new();
    out.push_str(&format!("func {}\n", function.name));
    for arg in &function.args {
        out.push_str(&format!("arg {}:{}\n", arg.name, arg.ty));
    }
    out.push_str(&format!("ret {}\n", function.ret));
    out.push_str(&format!("eff {}\n", function.effects.join(" ")));
    for capability in &function.capabilities {
        out.push_str(&format!("cap {}\n", capability));
    }
    for spec in &function.specs {
        out.push_str(&format!("spec {}\n", spec));
    }
    for block in &function.blocks {
        out.push_str(&format_block(block));
    }
    for test in &function.tests {
        out.push_str(&format!("test {}\n", test.name));
        if let Some(call) = &test.call {
            out.push_str(&format!("  call {}\n", call));
        }
        for (name, value) in &test.inputs {
            out.push_str(&format!("  in {}={}\n", name, value));
        }
        out.push_str(&format!("  out {}\n", test.expected));
    }
    out.push_str("end\n");
    out
}

fn format_block(block: &Block) -> String {
    let mut out = String::new();
    if block.params.is_empty() {
        out.push_str(&format!("block {}\n", block.label));
    } else {
        let params = block
            .params
            .iter()
            .map(|param| format!("{}:{}", param.name, param.ty))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!("block {}({})\n", block.label, params));
    }
    for instruction in &block.instructions {
        let suffix = if instruction.args.is_empty() {
            String::new()
        } else {
            format!(" {}", instruction.args.join(" "))
        };
        out.push_str(&format!(
            "  {}:{} = {}{}\n",
            instruction.bind, instruction.ty, instruction.op, suffix
        ));
    }
    match &block.terminator {
        Terminator::Return(value) => out.push_str(&format!("  return {}\n", value)),
        Terminator::Jump(target) => {
            out.push_str(&format!(
                "  jump {}\n",
                format_target(target.label.as_str(), &target.args)
            ));
        }
        Terminator::Branch {
            condition,
            truthy,
            falsy,
        } => out.push_str(&format!(
            "  branch {} {} {}\n",
            condition,
            format_target(truthy.label.as_str(), &truthy.args),
            format_target(falsy.label.as_str(), &falsy.args)
        )),
        Terminator::Match { value, arms } => out.push_str(&format!(
            "  match {} {}\n",
            value,
            arms.iter()
                .map(|target| format_target(target.label.as_str(), &target.args))
                .collect::<Vec<_>>()
                .join(" ")
        )),
    }
    out
}

fn format_target(label: &str, args: &[String]) -> String {
    if args.is_empty() {
        label.to_string()
    } else {
        format!("{}({})", label, args.join(", "))
    }
}
