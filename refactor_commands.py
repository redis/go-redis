#!/usr/bin/env python3
"""
Script to refactor go-redis commands to use pooling and return (value, error) tuples.
"""

import re
import sys

# Mapping of Cmd types to their return types and pool functions
CMD_TYPE_MAP = {
    '*StringCmd': ('string', 'getStringCmd', 'putStringCmd', '""'),
    '*IntCmd': ('int64', 'getIntCmd', 'putIntCmd', '0'),
    '*BoolCmd': ('bool', 'getBoolCmd', 'putBoolCmd', 'false'),
    '*FloatCmd': ('float64', 'getFloatCmd', 'putFloatCmd', '0'),
    '*StatusCmd': ('string', 'getStatusCmd', 'putStatusCmd', '""'),
    '*SliceCmd': ('[]interface{}', 'getSliceCmd', 'putSliceCmd', 'nil'),
    '*IntSliceCmd': ('[]int64', 'getIntSliceCmd', 'putIntSliceCmd', 'nil'),
    '*FloatSliceCmd': ('[]float64', 'getFloatSliceCmd', 'putFloatSliceCmd', 'nil'),
    '*StringSliceCmd': ('[]string', 'getStringSliceCmd', 'putStringSliceCmd', 'nil'),
    '*BoolSliceCmd': ('[]bool', 'getBoolSliceCmd', 'putBoolSliceCmd', 'nil'),
    '*DurationCmd': ('time.Duration', 'getDurationCmd', 'putDurationCmd', '0'),
    '*TimeCmd': ('time.Time', 'getTimeCmd', 'putTimeCmd', 'time.Time{}'),
    '*MapStringStringCmd': ('map[string]string', 'getMapStringStringCmd', 'putMapStringStringCmd', 'nil'),
    '*MapStringIntCmd': ('map[string]int64', 'getMapStringIntCmd', 'putMapStringIntCmd', 'nil'),
    '*MapStringInterfaceCmd': ('map[string]interface{}', 'getMapStringInterfaceCmd', 'putMapStringInterfaceCmd', 'nil'),
    '*ZSliceCmd': ('[]Z', 'getZSliceCmd', 'putZSliceCmd', 'nil'),
    '*DigestCmd': ('uint64', 'getDigestCmd', 'putDigestCmd', '0'),
}

def refactor_simple_function(match):
    """Refactor a simple function that creates a Cmd, processes it, and returns it."""
    indent = match.group(1)
    func_sig = match.group(2)
    cmd_type = match.group(3)
    cmd_args = match.group(4)
    
    # Extract return type from function signature
    if cmd_type not in CMD_TYPE_MAP:
        return match.group(0)  # Don't modify if we don't know the type
    
    ret_type, get_func, put_func, zero_val = CMD_TYPE_MAP[cmd_type]
    
    # Update function signature
    new_sig = re.sub(r'\) \*\w+Cmd$', f') ({ret_type}, error)', func_sig)
    
    # Generate new function body
    new_body = f'''{indent}{new_sig} {{
{indent}\tcmd := {get_func}()
{indent}\tcmd.baseCmd = baseCmd{{ctx: ctx, args: []interface{{{cmd_args}}}}}
{indent}\terr := c(ctx, cmd)
{indent}\tval, cmdErr := cmd.Val(), cmd.Err()
{indent}\t{put_func}(cmd)
{indent}\tif err != nil {{
{indent}\t\treturn {zero_val}, err
{indent}\t}}
{indent}\treturn val, cmdErr
{indent}}}'''
    
    return new_body

def main():
    if len(sys.argv) != 2:
        print("Usage: python refactor_commands.py <file.go>")
        sys.exit(1)
    
    filename = sys.argv[1]
    
    with open(filename, 'r') as f:
        content = f.read()
    
    # Pattern to match simple command functions
    # func (c cmdable) Name(...) *XxxCmd {
    #     cmd := NewXxxCmd(ctx, ...)
    #     _ = c(ctx, cmd)
    #     return cmd
    # }
    pattern = r'([ \t]*)' + \
              r'(func \(c cmdable\) \w+\([^)]+\)) (\*\w+Cmd) \{\n' + \
              r'\1\tcmd := New\w+Cmd\(ctx, ([^)]+)\)\n' + \
              r'\1\t_ = c\(ctx, cmd\)\n' + \
              r'\1\treturn cmd\n' + \
              r'\1\}'
    
    content = re.sub(pattern, refactor_simple_function, content, flags=re.MULTILINE)
    
    with open(filename, 'w') as f:
        f.write(content)
    
    print(f"Refactored {filename}")

if __name__ == '__main__':
    main()

