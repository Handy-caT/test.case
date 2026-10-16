pub fn get_basis_point(operand: f64, comparator: f64) -> f64 {
    if operand == 0.0 || comparator == 0.0 {
        return f64::NAN;
    }

    let difference = operand - comparator;
    let midpoint = (operand + comparator) / 2.0;
    (difference / midpoint) * 10_000.0
}
