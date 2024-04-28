use anyhow::{anyhow, Context, Result};
use core::fmt;
use std::str;

#[derive(Clone, Debug)]
pub enum RespType<'a> {
    SimpleStrings(&'a [u8]),
    SimpleErrors(&'a [u8]),
    Integers(i64),
    BulkStrings(&'a [u8]),
    Arrays(Vec<RespType<'a>>),
    Nulls,
    Booleans,
    Doubles,
    BigNumbers,
    BulkErrors,
    VerbatimStrings,
    Maps,
    Sets,
    Pushes,
}

impl<'a> RespType<'a> {}

pub fn parse_resp(buffer: &[u8]) -> Result<(RespType<'_>, &[u8])> {
    if buffer.is_empty() {
        unimplemented!();
    }
    match buffer.first().unwrap() {
        b'+' => parse_simple_strings(&buffer[1..]),
        b'-' => parse_simple_errors(&buffer[1..]),
        b':' => parse_integers(&buffer[1..]),
        b'$' => parse_bulk_strings(&buffer[1..]),
        b'*' => parse_arrays(&buffer[1..]),
        _ => unimplemented!(),
    }
}

fn parse_simple_strings(buffer: &[u8]) -> Result<(RespType<'_>, &[u8])> {
    let (a, b) = split_crlf_once(buffer)?;
    Ok((RespType::SimpleStrings(a), b))
}

fn parse_simple_errors(buffer: &[u8]) -> Result<(RespType<'_>, &[u8])> {
    let (a, b) = split_crlf_once(buffer)?;
    Ok((RespType::SimpleErrors(a), b))
}

fn parse_integers(buffer: &[u8]) -> Result<(RespType<'_>, &[u8])> {
    let (a, b) = split_crlf_once(buffer)?;
    #[rustfmt::skip]
    let sgn = if matches!(a.first(), Some(&x) if x == b'-') { -1i64 } else { 1i64 };
    let num = str::from_utf8(&a[1..]).unwrap().parse::<i64>().unwrap();
    Ok((RespType::Integers(sgn * num), b))
}

fn parse_bulk_strings(buffer: &[u8]) -> Result<(RespType<'_>, &[u8])> {
    let (a, b) = split_crlf_once(buffer)?;
    let len = str::from_utf8(a).unwrap().parse().unwrap();
    let (c, d) = split_crlf_at(b, len)?;
    Ok((RespType::BulkStrings(c), d))
}

fn parse_arrays(buffer: &[u8]) -> Result<(RespType<'_>, &[u8])> {
    let (a, b) = split_crlf_once(buffer)?;
    let num = str::from_utf8(a).unwrap().parse().unwrap();
    let mut res = Vec::with_capacity(num);
    let mut buffer = b;
    for _ in 0..num {
        if buffer.is_empty() {
            unreachable!();
        }
        let (a, b) = parse_resp(buffer)?;
        res.push(a);
        buffer = b;
    }
    Ok((RespType::Arrays(res), buffer))
}

// find the first CRLF and split
fn split_crlf_once(buffer: &[u8]) -> Result<(&[u8], &[u8])> {
    const CRLF: &[u8] = b"\r\n";
    let crlf = buffer
        .windows(CRLF.len())
        .position(|x| x == CRLF)
        .context("Cannot find CRLF!")?;
    Ok((&buffer[0..crlf], &buffer[crlf + CRLF.len()..]))
}

// split CRLF
fn split_crlf_at(buffer: &[u8], mid: usize) -> Result<(&[u8], &[u8])> {
    const CRLF: &[u8] = b"\r\n";
    if &buffer[mid..mid + CRLF.len()] != CRLF {
        Err(anyhow!("mid is not the starting position of CRLF."))
    } else {
        Ok((&buffer[0..mid], &buffer[mid + CRLF.len()..]))
    }
}

impl<'a> fmt::Display for RespType<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            RespType::SimpleStrings(x) => write!(f, "+{}\r\n", str::from_utf8(x).unwrap()),
            RespType::SimpleErrors(x) => write!(f, "-{}\r\n", str::from_utf8(x).unwrap()),
            RespType::Integers(x) => write!(f, ":{}\r\n", x),
            RespType::BulkStrings(x) => {
                write!(f, "${}\r\n{}\r\n", x.len(), str::from_utf8(x).unwrap())
            }
            RespType::Arrays(x) => {
                write!(f, "*{}\r\n", x.len())?;
                for xx in x.iter() {
                    fmt::Display::fmt(xx, f)?;
                }
                Ok(())
            }
            _ => unimplemented!(),
        }
    }
}
