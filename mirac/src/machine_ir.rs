use crate::lowered_bytecode::{
    BytecodeArg, BytecodeEdge, BytecodeExpr, BytecodeFunction, BytecodeInstruction,
    BytecodeMatchCase, BytecodeOperand, BytecodeProgram, BytecodeTerminator, BytecodeValueKind,
};

pub type MachineExpr = BytecodeExpr;
pub type MachineOperand = BytecodeOperand;
#[derive(Debug, Clone)]
pub struct MachineProgram {
    #[allow(dead_code)]
    pub module: String,
    pub functions: Vec<MachineFunction>,
}

#[derive(Debug, Clone)]
pub struct MachineFunction {
    pub name: String,
    pub arg_slots: Vec<BytecodeArg>,
    pub return_kind: BytecodeValueKind,
    #[allow(dead_code)]
    pub rand_seed: Option<u32>,
    pub slot_kinds: Vec<BytecodeValueKind>,
    pub slot_count: usize,
    pub entry_block: usize,
    pub blocks: Vec<MachineBlock>,
}

#[derive(Debug, Clone)]
pub struct MachineBlock {
    pub instructions: Vec<MachineInstruction>,
    pub terminator: MachineTerminator,
}

#[derive(Debug, Clone)]
pub struct MachineInstruction {
    pub dst: usize,
    pub dst_kind: BytecodeValueKind,
    pub expr: MachineExpr,
}

#[derive(Debug, Clone)]
pub struct MachineEdge {
    pub moves: Vec<MachineInstruction>,
    pub target: usize,
}

#[derive(Debug, Clone)]
pub struct MachineMatchCase {
    pub tag_index: usize,
    pub edge: MachineEdge,
}

#[derive(Debug, Clone)]
pub enum MachineTerminator {
    Return(MachineOperand),
    Jump(MachineEdge),
    Branch {
        condition: MachineOperand,
        truthy: MachineEdge,
        falsy: MachineEdge,
    },
    Match {
        value: MachineOperand,
        cases: Vec<MachineMatchCase>,
        default: MachineEdge,
    },
}

pub fn lower_bytecode_to_machine_program(program: &BytecodeProgram) -> MachineProgram {
    MachineProgram {
        module: program.module.clone(),
        functions: program
            .functions
            .iter()
            .map(lower_bytecode_function)
            .collect(),
    }
}

pub fn validate_machine_program(program: &MachineProgram) -> Result<(), String> {
    if program.functions.is_empty() {
        return Err("machine program has no functions".to_string());
    }
    for function in &program.functions {
        if function.entry_block >= function.blocks.len() {
            return Err(format!(
                "machine function {} has invalid entry block {}",
                function.name, function.entry_block
            ));
        }
        for block in &function.blocks {
            validate_machine_block(function, block)?;
        }
    }
    Ok(())
}

fn lower_bytecode_function(function: &BytecodeFunction) -> MachineFunction {
    MachineFunction {
        name: function.name.clone(),
        arg_slots: function.arg_slots.clone(),
        return_kind: function.return_kind,
        rand_seed: function.rand_seed,
        slot_kinds: function.slot_kinds.clone(),
        slot_count: function.slot_count,
        entry_block: function.entry_block,
        blocks: function.blocks.iter().map(lower_bytecode_block).collect(),
    }
}

fn lower_bytecode_block(block: &crate::lowered_bytecode::BytecodeBlock) -> MachineBlock {
    MachineBlock {
        instructions: block
            .instructions
            .iter()
            .map(lower_bytecode_instruction)
            .collect(),
        terminator: lower_bytecode_terminator(&block.terminator),
    }
}

fn lower_bytecode_instruction(instruction: &BytecodeInstruction) -> MachineInstruction {
    MachineInstruction {
        dst: instruction.dst,
        dst_kind: instruction.dst_kind,
        expr: instruction.expr.clone(),
    }
}

fn lower_bytecode_edge(edge: &BytecodeEdge) -> MachineEdge {
    MachineEdge {
        moves: edge.moves.iter().map(lower_bytecode_instruction).collect(),
        target: edge.target,
    }
}

fn lower_bytecode_terminator(terminator: &BytecodeTerminator) -> MachineTerminator {
    match terminator {
        BytecodeTerminator::Return(value) => MachineTerminator::Return(value.clone()),
        BytecodeTerminator::Jump(edge) => MachineTerminator::Jump(lower_bytecode_edge(edge)),
        BytecodeTerminator::Branch {
            condition,
            truthy,
            falsy,
        } => MachineTerminator::Branch {
            condition: condition.clone(),
            truthy: lower_bytecode_edge(truthy),
            falsy: lower_bytecode_edge(falsy),
        },
        BytecodeTerminator::Match {
            value,
            cases,
            default,
        } => MachineTerminator::Match {
            value: value.clone(),
            cases: cases
                .iter()
                .map(lower_bytecode_match_case)
                .collect::<Vec<_>>(),
            default: lower_bytecode_edge(default),
        },
    }
}

fn lower_bytecode_match_case(case: &BytecodeMatchCase) -> MachineMatchCase {
    MachineMatchCase {
        tag_index: case.tag_index,
        edge: lower_bytecode_edge(&case.edge),
    }
}

fn validate_machine_block(function: &MachineFunction, block: &MachineBlock) -> Result<(), String> {
    for instruction in &block.instructions {
        if instruction.dst >= function.slot_count {
            return Err(format!(
                "machine function {} writes invalid slot {}",
                function.name, instruction.dst
            ));
        }
    }
    validate_machine_terminator(function, &block.terminator)
}

fn validate_machine_terminator(
    function: &MachineFunction,
    terminator: &MachineTerminator,
) -> Result<(), String> {
    match terminator {
        MachineTerminator::Return(_) => Ok(()),
        MachineTerminator::Jump(edge) => validate_machine_edge(function, edge),
        MachineTerminator::Branch { truthy, falsy, .. } => {
            validate_machine_edge(function, truthy)?;
            validate_machine_edge(function, falsy)
        }
        MachineTerminator::Match { cases, default, .. } => {
            for case in cases {
                validate_machine_edge(function, &case.edge)?;
            }
            validate_machine_edge(function, default)
        }
    }
}

fn validate_machine_edge(function: &MachineFunction, edge: &MachineEdge) -> Result<(), String> {
    if edge.target >= function.blocks.len() {
        return Err(format!(
            "machine function {} jumps to invalid block {}",
            function.name, edge.target
        ));
    }
    for instruction in &edge.moves {
        if instruction.dst >= function.slot_count {
            return Err(format!(
                "machine function {} edge move writes invalid slot {}",
                function.name, instruction.dst
            ));
        }
    }
    Ok(())
}
