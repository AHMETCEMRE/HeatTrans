import sys
import os
import unittest
from pathlib import Path

# Adjust the path to import the Python assignment
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from python_assignment import simulate  # Import the simulate function or whatever you need to test

class TestPythonAssignment(unittest.TestCase):

    def test_simulation(self):
        project_dir = Path(__file__).parent.parent
        data_dir = project_dir / 'data'
        docs_dir = project_dir / 'docs'
        simulate(1)  # Run simulation for 1 day to limit the test scope
        output_path = data_dir / 'daily_operations.csv'
        with open(output_path, 'r') as file:
            lines = file.readlines()
            self.assertIn("Warehouse 0", lines[1]) #cSimple Check

if __name__ == '__main__':
    unittest.main()
