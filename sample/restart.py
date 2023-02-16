
from aiida.engine import run
from aiida.engine import process_handler, ProcessHandlerReport
from aiida.engine import while_
from aiida.engine import BaseRestartWorkChain
import pandas as pd
from aiida.engine import calcfunction, WorkChain
from aiida.orm import Int, Dict
from aiida.plugins import DataFactory
import aiida
aiida.load_profile()


FrameData = DataFactory('dataframe.frame')

_OUTPUT_DF = True


@calcfunction
def add_x_y(x, y):
    return x + y


class PositiveIntAddWorkChain(WorkChain):
    """WorkChain to multiply two numbers and add a third, for testing and demonstration purposes."""

    @classmethod
    def define(cls, spec):
        """Specify inputs and outputs."""
        super().define(spec)
        spec.input('x', valid_type=Int)
        spec.input('y', valid_type=Int)
        spec.input('params', valid_type=Dict, required=False)

        spec.outline(
            cls.add,
            cls.validate_results,
            cls.result,
        )
        spec.output('result', valid_type=Int)
        if _OUTPUT_DF:
            spec.output('history', valid_type=FrameData)
        # define exit code used in this class.
        # It will be accessed by self.exit_codes.ERROR_NEGATIVE_NUMBER.
        spec.exit_code(400, 'ERROR_NEGATIVE_NUMBER', message='The result is a negative number.')

    def validate_results(self):
        """Make sure the result is not negative."""
        print("validate_results", self.ctx.sum)
        self.out('result', self.ctx.sum)
        if self.ctx.sum <= 0:
            print("error", self.exit_codes.ERROR_NEGATIVE_NUMBER)
            return self.exit_codes.ERROR_NEGATIVE_NUMBER

    def add(self):
        """Multiply two integers. returns x*y"""
        self.ctx.sum = add_x_y(self.inputs.x, self.inputs.y)

    def result(self):
        """Add the result to the outputs."""
        self.out('result', self.ctx.sum)
        if _OUTPUT_DF:
            df = pd.DataFrame([[1, 2, 3], [3, 34, 5]], columns=['a', 'b', 'c'])
            self.out('history', FrameData(df).store())


addworkchain = PositiveIntAddWorkChain
params = {"incx": Int(1), "incy": Int(2)}
results = run(addworkchain, x=Int(2), y=Int(5))  # OK
print(results)


results = run(addworkchain, x=Int(2), y=Int(-5))  # OK
print(results)


class AddCorrectionBaseWorkChain(BaseRestartWorkChain):

    _process_class = PositiveIntAddWorkChain

    @classmethod
    def define(cls, spec):
        """Define the process specification."""
        super().define(spec)

        spec.expose_inputs(cls._process_class)
        spec.expose_outputs(cls._process_class)

        spec.outline(
            cls.setup,
            while_(cls.should_run_process)(
                cls.run_process,
                cls.inspect_process,
            ),
            cls.results,
        )
        spec.exit_code(500, 'ERROR_NO_RECOVERY', message='The product is a negative number.')

    def setup(self):
        """Call the `setup` of the `BaseRestartWorkChain` and then create the inputs dictionary in `self.ctx.inputs`.

        This `self.ctx.inputs` dictionary will be used by the `BaseRestartWorkChain` to submit the process in the
        internal loop.
        """
        super().setup()
        self.ctx.inputs = {'x': self.inputs.x, 'y': self.inputs.y, 'params': self.inputs.params}

    @process_handler(priority=500, exit_codes=_process_class.exit_codes.ERROR_NEGATIVE_NUMBER)
    def handle_negative_result(self, node):
        """Check if the calculation failed with `ERROR_X_NEGATIVE_NUMBER`.

        If this is the case, simply make the inputs positive by taking the absolute value.

        :param node: the node of the subprocess that was ran in the current iteration.
        :return: optional :class:`~aiida.engine.processes.workchains.utils.ProcessHandlerReport` instance to signal
            that a problem was detected and potentially handled.
        """
        print("result negative")
        if node.exit_status == self._process_class.exit_codes.ERROR_NEGATIVE_NUMBER.status:
            incy = node.inputs.params.get_dict()["incy"]
            incx = node.inputs.params.get_dict()["incx"]
            print("output.result", node.outputs, node.outputs.result)
            print("node", node)

            self.ctx.inputs['x'] = Int(node.inputs.x.value+incx)
            self.ctx.inputs['y'] = Int(node.inputs.y.value+incy)
            print("add x,y", incx, incy)
            print("change x,y", self.ctx.inputs['x'].value, self.ctx.inputs['y'].value)
            return ProcessHandlerReport()

        return ProcessHandlerReport(exit_code=self.exit_codes.ERROR_NO_RECOVERY)


restartaddworkchain = AddCorrectionBaseWorkChain
result = run(restartaddworkchain, x=Int(-2), y=Int(-4),
             max_iterations=Int(10),
             params=Dict(dict=params)
             )
print(result) # NG
