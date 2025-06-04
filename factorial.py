"""Simple factorial function"""

def factorial(n):
    """Return the factorial of a non-negative integer n."""
    if not isinstance(n, int):
        raise TypeError("n must be an integer")
    if n < 0:
        raise ValueError("n must be >= 0")
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python factorial.py <non-negative integer>")
        sys.exit(1)
    arg = int(sys.argv[1])
    print(factorial(arg))

