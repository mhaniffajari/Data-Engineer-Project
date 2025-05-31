import ast
from inspect import getsource

import click
from dbt.cli import params
from dbt.cli.main import cli


class TestCLI:
    def test_commands_have_docstrings(self):
        def run_test(commands):
            for command in commands.values():
                if type(command) is click.Command:
                    assert command.__doc__ is not None
                if type(command) is click.Group:
                    run_test(command.commands)

        run_test(cli.commands)

    # TODO:  This isn't the ideal way to test params as
    # they will be tested as many times as they are used as decorators.
    # This is inefficent (obvs)
    def test_unhidden_params_have_help_texts(self):
        def run_test(command):
            for param in command.params:
                if not param.hidden:
                    assert param.help is not None
            if type(command) is click.Group:
                for command in command.commands.values():
                    run_test(command)

        run_test(cli)

    def test_param_names_match_envvars(self):
        def run_test(command):
            for param in command.params:
                if param.envvar is not None:
                    assert "DBT_" + param.name.upper() == param.envvar
            if type(command) is click.Group:
                for command in command.commands.values():
                    run_test(command)

        run_test(cli)

    def test_params_are_alpha_sorted(self):
        root_node = ast.parse(getsource(params))
        param_var_names = [
            node.targets[0].id for node in ast.walk(root_node) if isinstance(node, ast.Assign)
        ]
        assert param_var_names == sorted(param_var_names)
