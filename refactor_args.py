#!/usr/bin/env python3
import re
import sys

def refactor_string_commands(content):
    """
    Refactor string_commands.go to use setArgs instead of creating new slices

    Changes:
    cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"get", key}}

    To:
    cmd.ctx = ctx
    cmd.args = setArgs(cmd.args, "get", key)
    """

    # Pattern to match: cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{...}}
    pattern = r'cmd\.baseCmd = baseCmd\{ctx: ctx, args: (\[\]interface\{\{[^}]+\}|args|cmdArgs)\}'

    def replace_func(match):
        args_part = match.group(1)

        # If it's a variable (args or cmdArgs), keep it as is
        if args_part in ['args', 'cmdArgs']:
            return f'cmd.ctx = ctx\n\tcmd.args = {args_part}'

        # Extract the arguments from []interface{}{...}
        # Pattern: []interface{}{"cmd", arg1, arg2, ...}
        args_match = re.search(r'\[\]interface\{\}\{([^}]+)\}', args_part)
        if args_match:
            args_str = args_match.group(1)
            return f'cmd.ctx = ctx\n\tcmd.args = setArgs(cmd.args, {args_str})'

        return match.group(0)  # Return original if we can't parse

    # Replace all occurrences
    result = re.sub(pattern, replace_func, content)

    return result

if __name__ == '__main__':
    filename = 'string_commands.go'
    
    with open(filename, 'r') as f:
        content = f.read()
    
    refactored = refactor_string_commands(content)
    
    with open(filename, 'w') as f:
        f.write(refactored)
    
    print(f"Refactored {filename}")

