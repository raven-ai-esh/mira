use std::{collections::HashMap, fmt};

use crate::ast::{Field, TypeDeclBody};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum TypeRef {
    Int { signed: bool, bits: u16 },
    Float { bits: u16 },
    Bool,
    String,
    Named(String),
    Span(Box<TypeRef>),
    Buf(Box<TypeRef>),
    Vec { len: usize, elem: Box<TypeRef> },
    Option(Box<TypeRef>),
    Result { ok: Box<TypeRef>, err: Box<TypeRef> },
    Own(Box<TypeRef>),
    View(Box<TypeRef>),
    Edit(Box<TypeRef>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamedFieldValue {
    pub name: String,
    pub value: DataValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum DataValue {
    Int(i128),
    Float(f64),
    Bool(bool),
    Symbol(String),
    Array(Vec<DataValue>),
    Fields(Vec<NamedFieldValue>),
    Variant {
        name: String,
        fields: Vec<NamedFieldValue>,
    },
}

impl TypeRef {
    pub fn parse(text: &str) -> Result<Self, String> {
        TypeParser::new(text).parse()
    }

    pub fn c_type(&self) -> Result<String, String> {
        match self {
            Self::Int {
                signed: true,
                bits: 8,
            } => Ok("int8_t".to_string()),
            Self::Int {
                signed: true,
                bits: 16,
            } => Ok("int16_t".to_string()),
            Self::Int {
                signed: true,
                bits: 32,
            } => Ok("int32_t".to_string()),
            Self::Int {
                signed: true,
                bits: 64,
            } => Ok("int64_t".to_string()),
            Self::Int {
                signed: true,
                bits: 128,
            } => Ok("__int128".to_string()),
            Self::Int {
                signed: false,
                bits: 8,
            } => Ok("uint8_t".to_string()),
            Self::Int {
                signed: false,
                bits: 16,
            } => Ok("uint16_t".to_string()),
            Self::Int {
                signed: false,
                bits: 32,
            } => Ok("uint32_t".to_string()),
            Self::Int {
                signed: false,
                bits: 64,
            } => Ok("uint64_t".to_string()),
            Self::Int {
                signed: false,
                bits: 128,
            } => Ok("unsigned __int128".to_string()),
            Self::Float { bits: 16 } => Ok("_Float16".to_string()),
            Self::Float { bits: 32 } => Ok("float".to_string()),
            Self::Float { bits: 64 } => Ok("double".to_string()),
            Self::Bool => Ok("bool".to_string()),
            Self::String => Ok("buf_u8".to_string()),
            Self::Named(name) => Ok(format!("mira_named_{}", sanitize_identifier(name))),
            Self::Span(inner) => Ok(format!("span_{}", inner.type_key()?)),
            Self::Buf(inner) => Ok(format!("buf_{}", inner.type_key()?)),
            Self::Vec { len, elem } => Ok(format!("vec_{}_{}", len, elem.type_key()?)),
            Self::Option(inner) => Ok(format!("option_{}", inner.type_key()?)),
            Self::Result { ok, err } => {
                Ok(format!("result_{}_{}", ok.type_key()?, err.type_key()?))
            }
            Self::Own(inner) | Self::View(inner) | Self::Edit(inner) => inner.c_type(),
            _ => Err(format!("unsupported C lowering for type {self}")),
        }
    }

    pub fn type_key(&self) -> Result<String, String> {
        match self {
            Self::Int { signed: true, bits } => Ok(format!("i{bits}")),
            Self::Int {
                signed: false,
                bits,
            } => Ok(format!("u{bits}")),
            Self::Float { bits } => Ok(format!("f{bits}")),
            Self::Bool => Ok("b1".to_string()),
            Self::String => Ok("str".to_string()),
            Self::Named(name) => Ok(format!("named_{}", sanitize_identifier(name))),
            Self::Span(inner) => Ok(format!("span_{}", inner.type_key()?)),
            Self::Buf(inner) => Ok(format!("buf_{}", inner.type_key()?)),
            Self::Vec { len, elem } => Ok(format!("vec_{}_{}", len, elem.type_key()?)),
            Self::Option(inner) => Ok(format!("option_{}", inner.type_key()?)),
            Self::Result { ok, err } => {
                Ok(format!("result_{}_{}", ok.type_key()?, err.type_key()?))
            }
            Self::Own(inner) => Ok(format!("own_{}", inner.type_key()?)),
            Self::View(inner) => Ok(format!("view_{}", inner.type_key()?)),
            Self::Edit(inner) => Ok(format!("edit_{}", inner.type_key()?)),
        }
    }

    pub fn is_bool(&self) -> bool {
        matches!(self, Self::Bool)
    }

    pub fn is_int(&self) -> bool {
        matches!(self, Self::Int { .. })
    }

    pub fn is_signed_int(&self) -> bool {
        matches!(self, Self::Int { signed: true, .. })
    }

    pub fn is_float(&self) -> bool {
        matches!(self, Self::Float { .. })
    }

    pub fn is_numeric(&self) -> bool {
        self.is_int() || self.is_float()
    }
}

impl fmt::Display for TypeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int { signed: true, bits } => write!(f, "i{bits}"),
            Self::Int {
                signed: false,
                bits,
            } => write!(f, "u{bits}"),
            Self::Float { bits } => write!(f, "f{bits}"),
            Self::Bool => write!(f, "b1"),
            Self::String => write!(f, "str"),
            Self::Named(name) => write!(f, "{name}"),
            Self::Span(inner) => write!(f, "span[{inner}]"),
            Self::Buf(inner) => write!(f, "buf[{inner}]"),
            Self::Vec { len, elem } => write!(f, "vec[{len},{elem}]"),
            Self::Option(inner) => write!(f, "option[{inner}]"),
            Self::Result { ok, err } => write!(f, "result[{ok},{err}]"),
            Self::Own(inner) => write!(f, "own[{inner}]"),
            Self::View(inner) => write!(f, "view[{inner}]"),
            Self::Edit(inner) => write!(f, "edit[{inner}]"),
        }
    }
}

pub fn infer_literal_type(token: &str) -> Option<TypeRef> {
    let token = token.trim();
    if token == "true" || token == "false" {
        return Some(TypeRef::Bool);
    }
    let (_, suffix) = split_number_suffix(token)?;
    suffix.and_then(|suffix| TypeRef::parse(suffix).ok())
}

pub fn split_number_suffix(token: &str) -> Option<(String, Option<&str>)> {
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    for suffix in scalar_suffixes() {
        if let Some(number) = token.strip_suffix(suffix) {
            if !number.is_empty() && looks_like_number(number) {
                return Some((number.to_string(), Some(suffix)));
            }
        }
    }
    if looks_like_number(token) {
        return Some((token.to_string(), None));
    }
    None
}

pub fn split_top_level(text: &str, delimiter: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut bracket_depth = 0usize;
    let mut paren_depth = 0usize;
    for ch in text.chars() {
        match ch {
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            _ => {}
        }
        if ch == delimiter && bracket_depth == 0 && paren_depth == 0 {
            let part = current.trim();
            if !part.is_empty() {
                parts.push(part.to_string());
            }
            current.clear();
            continue;
        }
        current.push(ch);
    }
    let part = current.trim();
    if !part.is_empty() {
        parts.push(part.to_string());
    }
    parts
}

pub fn split_top_level_whitespace(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut bracket_depth = 0usize;
    let mut paren_depth = 0usize;
    for ch in text.chars() {
        match ch {
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            _ => {}
        }
        if ch.is_whitespace() && bracket_depth == 0 && paren_depth == 0 {
            let part = current.trim();
            if !part.is_empty() {
                parts.push(part.to_string());
            }
            current.clear();
            continue;
        }
        current.push(ch);
    }
    let part = current.trim();
    if !part.is_empty() {
        parts.push(part.to_string());
    }
    parts
}

pub fn parse_data_literal(
    text: &str,
    expected: &TypeRef,
    named_types: Option<&HashMap<String, TypeDeclBody>>,
) -> Result<DataValue, String> {
    let text = text.trim();
    match expected {
        TypeRef::Bool => match text {
            "true" => Ok(DataValue::Bool(true)),
            "false" => Ok(DataValue::Bool(false)),
            _ => Err(format!("expected bool literal, got {text}")),
        },
        TypeRef::Int { .. } => {
            let (number, suffix) = split_number_suffix(text)
                .ok_or_else(|| format!("expected integer literal, got {text}"))?;
            if let Some(suffix) = suffix {
                let suffix_type = TypeRef::parse(suffix)?;
                if &suffix_type != expected {
                    return Err(format!("literal {text} does not match {expected}"));
                }
            }
            let value = number
                .parse::<i128>()
                .map_err(|_| format!("invalid integer literal {text}"))?;
            Ok(DataValue::Int(value))
        }
        TypeRef::Float { .. } => {
            let (number, suffix) = split_number_suffix(text)
                .ok_or_else(|| format!("expected float literal, got {text}"))?;
            if let Some(suffix) = suffix {
                let suffix_type = TypeRef::parse(suffix)?;
                if &suffix_type != expected {
                    return Err(format!("literal {text} does not match {expected}"));
                }
            }
            let value = number
                .parse::<f64>()
                .map_err(|_| format!("invalid float literal {text}"))?;
            Ok(DataValue::Float(value))
        }
        TypeRef::Named(name) => parse_named_literal(text, name, named_types),
        TypeRef::Span(inner) | TypeRef::Buf(inner) => parse_array_literal(text, inner, named_types),
        TypeRef::Vec { elem, .. } => parse_array_literal(text, elem, named_types),
        _ => Err(format!("data literals are not implemented for {expected}")),
    }
}

pub fn render_c_literal(token: &str, expected: Option<&TypeRef>) -> Result<String, String> {
    let token = token.trim();
    if token == "true" || token == "false" {
        return Ok(token.to_string());
    }
    if let Some((number, suffix)) = split_number_suffix(token) {
        let ty = if let Some(suffix) = suffix {
            TypeRef::parse(suffix)?
        } else if let Some(expected) = expected.cloned() {
            expected
        } else {
            return Ok(number);
        };
        return match ty {
            TypeRef::Int { .. } => render_c_number(&number, &ty),
            TypeRef::Float { .. } => render_c_float(&number, &ty),
            TypeRef::Named(name) => render_named_literal(token, &name),
            _ => Err(format!("cannot render scalar literal {token} as {ty}")),
        };
    }
    if let Some(TypeRef::Named(name)) = expected {
        return render_named_literal(token, name);
    }
    Err(format!("unsupported literal token {token}"))
}

pub fn render_data_value(value: &DataValue, ty: &TypeRef) -> Result<String, String> {
    match (value, ty) {
        (DataValue::Bool(value), TypeRef::Bool) => {
            Ok(if *value { "true" } else { "false" }.to_string())
        }
        (DataValue::Int(value), TypeRef::Int { .. }) => render_c_number(&value.to_string(), ty),
        (DataValue::Float(value), TypeRef::Float { .. }) => render_c_float(&value.to_string(), ty),
        (DataValue::Symbol(value), TypeRef::Named(name)) => render_named_literal(value, name),
        _ => Err(format!("cannot render {value:?} as {ty}")),
    }
}

fn parse_named_literal(
    text: &str,
    type_name: &str,
    named_types: Option<&HashMap<String, TypeDeclBody>>,
) -> Result<DataValue, String> {
    let Some(named_types) = named_types else {
        let prefix = format!("{type_name}.");
        if text.starts_with(&prefix) {
            return Ok(DataValue::Symbol(text.to_string()));
        }
        return Err(format!(
            "named literal {text} requires declaration context for {type_name}"
        ));
    };
    let body = named_types
        .get(type_name)
        .ok_or_else(|| format!("unknown named type {type_name}"))?;
    match body {
        TypeDeclBody::Struct { fields } => {
            let payload = text
                .strip_prefix(type_name)
                .and_then(|rest| rest.strip_prefix('['))
                .and_then(|rest| rest.strip_suffix(']'))
                .ok_or_else(|| {
                    format!("expected struct literal {type_name}[field=value,...], got {text}")
                })?;
            Ok(DataValue::Fields(parse_named_field_values(
                payload,
                fields,
                named_types,
            )?))
        }
        TypeDeclBody::Enum { variants } => {
            let prefix = format!("{type_name}.");
            let rest = text
                .strip_prefix(&prefix)
                .ok_or_else(|| format!("expected enum literal {type_name}.variant, got {text}"))?;
            if let Some(payload_text) = rest.strip_suffix(']') {
                let (variant_name, fields_text) =
                    payload_text.split_once('[').ok_or_else(|| {
                        format!(
                            "expected payload enum literal {type_name}.variant[field=value,...], got {text}"
                        )
                    })?;
                let variant = variants
                    .iter()
                    .find(|variant| variant.name == variant_name)
                    .ok_or_else(|| format!("unknown variant {variant_name} on {type_name}"))?;
                if variant.fields.is_empty() {
                    return Err(format!(
                        "variant {type_name}.{variant_name} does not carry payload"
                    ));
                }
                return Ok(DataValue::Variant {
                    name: variant_name.to_string(),
                    fields: parse_named_field_values(fields_text, &variant.fields, named_types)?,
                });
            }
            let variant = variants
                .iter()
                .find(|variant| rest == variant.name)
                .ok_or_else(|| format!("unknown variant {rest} on {type_name}"))?;
            if !variant.fields.is_empty() {
                return Err(format!(
                    "variant {type_name}.{} requires payload fields",
                    variant.name
                ));
            }
            Ok(DataValue::Symbol(text.to_string()))
        }
    }
}

fn parse_named_field_values(
    text: &str,
    fields: &[Field],
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<Vec<NamedFieldValue>, String> {
    if fields.is_empty() {
        if text.trim().is_empty() {
            return Ok(Vec::new());
        }
        return Err("payload list must be empty".to_string());
    }
    let parts = if text.trim().is_empty() {
        Vec::new()
    } else {
        split_top_level(text, ',')
    };
    if parts.len() != fields.len() {
        return Err(format!(
            "expected {} named fields, got {}",
            fields.len(),
            parts.len()
        ));
    }
    let mut values = Vec::with_capacity(fields.len());
    for (part, field) in parts.iter().zip(fields.iter()) {
        let (name, value_text) = part
            .split_once('=')
            .ok_or_else(|| format!("expected named field assignment, got {part}"))?;
        if name.trim() != field.name {
            return Err(format!(
                "expected field {} in canonical order, got {}",
                field.name,
                name.trim()
            ));
        }
        values.push(NamedFieldValue {
            name: field.name.clone(),
            value: parse_data_literal(value_text.trim(), &field.ty, Some(named_types))?,
        });
    }
    Ok(values)
}

fn parse_array_literal(
    text: &str,
    inner: &TypeRef,
    named_types: Option<&HashMap<String, TypeDeclBody>>,
) -> Result<DataValue, String> {
    let inner_text = text
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .ok_or_else(|| format!("expected array literal, got {text}"))?;
    if inner_text.trim().is_empty() {
        return Ok(DataValue::Array(Vec::new()));
    }
    let mut values = Vec::new();
    for part in split_top_level(inner_text, ',') {
        values.push(parse_data_literal(&part, inner, named_types)?);
    }
    Ok(DataValue::Array(values))
}

fn render_c_number(number: &str, ty: &TypeRef) -> Result<String, String> {
    match ty {
        TypeRef::Int {
            signed: true,
            bits: 128,
        } => Ok(format!("((__int128){number})")),
        TypeRef::Int {
            signed: false,
            bits: 128,
        } => Ok(format!("((unsigned __int128){number}u)")),
        TypeRef::Int { signed: true, bits } => Ok(format!("((int{bits}_t){number})")),
        TypeRef::Int {
            signed: false,
            bits,
        } => Ok(format!("((uint{bits}_t){number}u)")),
        _ => Err(format!("cannot render scalar number for {ty}")),
    }
}

fn render_c_float(number: &str, ty: &TypeRef) -> Result<String, String> {
    match ty {
        TypeRef::Float { bits: 16 } => Ok(format!("((_Float16){number})")),
        TypeRef::Float { bits: 32 } => Ok(format!("((float){number})")),
        TypeRef::Float { bits: 64 } => Ok(format!("((double){number})")),
        _ => Err(format!("cannot render scalar float for {ty}")),
    }
}

fn render_named_literal(token: &str, type_name: &str) -> Result<String, String> {
    let prefix = format!("{type_name}.");
    let variant = token
        .strip_prefix(&prefix)
        .ok_or_else(|| format!("expected enum literal {type_name}.variant, got {token}"))?;
    Ok(format!(
        "mira_enum_{}_{}",
        sanitize_identifier(type_name),
        sanitize_identifier(variant)
    ))
}

fn looks_like_number(text: &str) -> bool {
    text.parse::<i128>().is_ok() || text.parse::<f64>().is_ok()
}

pub fn sanitize_identifier(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "_".to_string()
    } else {
        out
    }
}

fn scalar_suffixes() -> &'static [&'static str] {
    &[
        "i128", "u128", "f64", "f32", "f16", "i64", "u64", "i32", "u32", "i16", "u16", "i8", "u8",
    ]
}

struct TypeParser<'a> {
    text: &'a str,
    index: usize,
}

impl<'a> TypeParser<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            text: text.trim(),
            index: 0,
        }
    }

    fn parse(mut self) -> Result<TypeRef, String> {
        let ty = self.parse_type()?;
        self.consume_ws();
        if self.index != self.text.len() {
            return Err(format!(
                "unexpected trailing type text: {}",
                &self.text[self.index..]
            ));
        }
        Ok(ty)
    }

    fn parse_type(&mut self) -> Result<TypeRef, String> {
        self.consume_ws();
        let ident = self.parse_ident()?;
        self.consume_ws();
        if !self.peek("[") {
            return parse_scalar_ident(&ident);
        }
        self.expect("[")?;
        let ty = match ident.as_str() {
            "vec" => {
                self.consume_ws();
                let len = self.parse_usize()?;
                self.consume_ws();
                self.expect(",")?;
                let elem = self.parse_type()?;
                self.consume_ws();
                self.expect("]")?;
                TypeRef::Vec {
                    len,
                    elem: Box::new(elem),
                }
            }
            "result" => {
                let ok = self.parse_type()?;
                self.consume_ws();
                self.expect(",")?;
                let err = self.parse_type()?;
                self.consume_ws();
                self.expect("]")?;
                TypeRef::Result {
                    ok: Box::new(ok),
                    err: Box::new(err),
                }
            }
            "span" | "buf" | "option" | "own" | "view" | "edit" => {
                let inner = self.parse_type()?;
                self.consume_ws();
                self.expect("]")?;
                match ident.as_str() {
                    "span" => TypeRef::Span(Box::new(inner)),
                    "buf" => TypeRef::Buf(Box::new(inner)),
                    "option" => TypeRef::Option(Box::new(inner)),
                    "own" => TypeRef::Own(Box::new(inner)),
                    "view" => TypeRef::View(Box::new(inner)),
                    "edit" => TypeRef::Edit(Box::new(inner)),
                    _ => unreachable!(),
                }
            }
            _ => return Err(format!("unsupported type constructor {ident}")),
        };
        Ok(ty)
    }

    fn parse_ident(&mut self) -> Result<String, String> {
        let start = self.index;
        while self.index < self.text.len() {
            let ch = self.text[self.index..].chars().next().unwrap_or_default();
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' {
                self.index += ch.len_utf8();
            } else {
                break;
            }
        }
        if start == self.index {
            return Err(format!(
                "expected type identifier near: {}",
                &self.text[self.index..]
            ));
        }
        Ok(self.text[start..self.index].to_string())
    }

    fn parse_usize(&mut self) -> Result<usize, String> {
        let start = self.index;
        while self.index < self.text.len() {
            let ch = self.text[self.index..].chars().next().unwrap_or_default();
            if ch.is_ascii_digit() {
                self.index += ch.len_utf8();
            } else {
                break;
            }
        }
        if start == self.index {
            return Err(format!(
                "expected integer near: {}",
                &self.text[self.index..]
            ));
        }
        self.text[start..self.index]
            .parse::<usize>()
            .map_err(|_| format!("invalid vec size near: {}", &self.text[start..self.index]))
    }

    fn consume_ws(&mut self) {
        while self.index < self.text.len() {
            let ch = self.text[self.index..].chars().next().unwrap_or_default();
            if ch.is_whitespace() {
                self.index += ch.len_utf8();
            } else {
                break;
            }
        }
    }

    fn peek(&self, value: &str) -> bool {
        self.text[self.index..].starts_with(value)
    }

    fn expect(&mut self, value: &str) -> Result<(), String> {
        if self.peek(value) {
            self.index += value.len();
            Ok(())
        } else {
            Err(format!(
                "expected '{value}' near: {}",
                &self.text[self.index..]
            ))
        }
    }
}

fn parse_scalar_ident(ident: &str) -> Result<TypeRef, String> {
    if ident == "b1" {
        return Ok(TypeRef::Bool);
    }
    if ident == "str" {
        return Ok(TypeRef::String);
    }
    if let Some(bits) = ident.strip_prefix('i') {
        return parse_bits(bits).map(|bits| TypeRef::Int { signed: true, bits });
    }
    if let Some(bits) = ident.strip_prefix('u') {
        return parse_bits(bits).map(|bits| TypeRef::Int {
            signed: false,
            bits,
        });
    }
    if let Some(bits) = ident.strip_prefix('f') {
        return parse_float_bits(bits).map(|bits| TypeRef::Float { bits });
    }
    Ok(TypeRef::Named(ident.to_string()))
}

fn parse_bits(bits: &str) -> Result<u16, String> {
    let bits = bits
        .parse::<u16>()
        .map_err(|_| format!("invalid integer type i{bits}"))?;
    match bits {
        8 | 16 | 32 | 64 | 128 => Ok(bits),
        _ => Err(format!("unsupported integer width {bits}")),
    }
}

fn parse_float_bits(bits: &str) -> Result<u16, String> {
    let bits = bits
        .parse::<u16>()
        .map_err(|_| format!("invalid float type f{bits}"))?;
    match bits {
        16 | 32 | 64 => Ok(bits),
        _ => Err(format!("unsupported float width {bits}")),
    }
}
