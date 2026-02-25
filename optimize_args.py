#!/usr/bin/env python3
"""
Optimize string_commands.go to use setArgs2, setArgs3, setArgs4 instead of variadic setArgs
This eliminates variadic slice allocations
"""

import re

def count_args(args_str):
    """Count the number of arguments in the args string"""
    # Remove outer braces and split by comma
    args_str = args_str.strip()
    if args_str.startswith('[]interface{}{') and args_str.endswith('}'):
        inner = args_str[14:-1]  # Remove []interface{}{...}
        if not inner:
            return 0
        # Simple count - split by comma and count
        parts = inner.split(',')
        return len(parts)
    return None

def optimize_file(filename):
    with open(filename, 'r') as f:
        content = f.read()
    
    original = content
    
    # Pattern 1: cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{...}}
    pattern1 = r'cmd\.baseCmd = baseCmd\{ctx: ctx, args: (\[\]interface\{\}\{[^}]+\})\}'
    
    def replace1(match):
        args_str = match.group(1)
        # Extract the arguments
        if args_str.startswith('[]interface{}{') and args_str.endswith('}'):
            inner = args_str[14:-1]
            # Count commas to determine number of args
            arg_count = len([x for x in inner.split(',') if x.strip()])
            
            if arg_count == 2:
                return f'cmd.ctx = ctx\n\tcmd.args = setArgs2(cmd.args, {inner})'
            elif arg_count == 3:
                return f'cmd.ctx = ctx\n\tcmd.args = setArgs3(cmd.args, {inner})'
            elif arg_count == 4:
                return f'cmd.ctx = ctx\n\tcmd.args = setArgs4(cmd.args, {inner})'
        return match.group(0)
    
    content = re.sub(pattern1, replace1, content)
    
    # Pattern 2: cmd.args = setArgs(cmd.args, ...) with 2-4 args
    # This is trickier - we need to count the arguments
    pattern2 = r'cmd\.args = setArgs\(cmd\.args, ([^)]+)\)'
    
    def replace2(match):
        args_str = match.group(1)
        # Count arguments (simple heuristic - count commas + 1)
        # This won't work for nested function calls, but should work for simple cases
        arg_count = len([x for x in args_str.split(',') if x.strip()]) 
        
        if arg_count == 2:
            return f'cmd.args = setArgs2(cmd.args, {args_str})'
        elif arg_count == 3:
            return f'cmd.args = setArgs3(cmd.args, {args_str})'
        elif arg_count == 4:
            return f'cmd.args = setArgs4(cmd.args, {args_str})'
        return match.group(0)
    
    content = re.sub(pattern2, replace2, content)
    
    if content != original:
        with open(filename, 'w') as f:
            f.write(content)
        print(f"Optimized {filename}")
        return True
    else:
        print(f"No changes needed for {filename}")
        return False

if __name__ == '__main__':
    optimize_file('string_commands.go')

