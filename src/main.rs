mod parser;

fn main() {
    let inp = "select [ab c] as a, def as d from [g hi], ./jkl.csv as j where max(abc) >= 5 group by mno, pqr having mno > 0";

    println!("{:?}", parser::parse(inp));
}
