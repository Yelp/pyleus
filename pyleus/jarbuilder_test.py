import os

import testify as T
import jarbuilder as P


class JarbuilderTest(T.TestCase):

    def test__build_otuput_path(self):
        topology_dir = "foo"

        output_arg = "ninja"
        output_jar = P._build_output_path(output_arg, topology_dir)
        T.assert_equals(output_jar, os.path.abspath(output_arg))

        output_arg = None
        output_jar = P._build_output_path(output_arg, topology_dir)
        T.assert_equals(output_jar, os.path.abspath(topology_dir) + '.jar')

if __name__ == '__main__':
        T.run()
