import csv
from pathlib import Path

def read_initial_data(filename):
    warehouses = {}
    current_key = None
    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()
            if line.startswith('Warehouse Unit:'):
                current_key = int(line.split(':')[-1].strip())
                warehouses[current_key] = []
            elif line.startswith('Starting Products:'):
                products = line.split(':')[-1].strip()
                warehouses[current_key] = list(map(int, products.split(',')))
    return warehouses

# Define the warehouse transformations and tests
formulas = {
    0: lambda x: x * 7,
    1: lambda x: x * x,
    2: lambda x: x + 8,
    3: lambda x: x + 4,
    4: lambda x: x + 3,
    5: lambda x: x + 5,
    6: lambda x: x + 7,
    7: lambda x: x * 3,
}

tests = {
    0: lambda x: (x % 5 == 0, 1, 6),
    1: lambda x: (x % 17 == 0, 2, 5),
    2: lambda x: (x % 7 == 0, 4, 3),
    3: lambda x: (x % 13 == 0, 0, 7),
    4: lambda x: (x % 19 == 0, 7, 3),
    5: lambda x: (x % 3 == 0, 4, 2),
    6: lambda x: (x % 11 == 0, 1, 5),
    7: lambda x: (x % 2 == 0, 0, 6),
}

# Define the simulation function
def simulate(days=10):
    # Set the base directories for project data and documents using pathlib for OS independent paths
    project_dir = Path(__file__).parent.parent
    data_dir = project_dir / 'data'
    docs_dir = project_dir / 'docs'
    
    # Load initial warehouse data from a text file in the docs directory
    warehouses = read_initial_data(docs_dir / 'dataset_assignment_data_engineer.txt')
    results = []  # Initialize a list to store the results of each day's operations

    # Simulate the operations for a given number of days
    for day in range(1, days + 1):
        new_warehouses = {key: [] for key in warehouses}  # Prepare a new structure for the next day's product storage
        
        # Process each warehouse's products
        for wh_id, products in warehouses.items():
            for product in products:
                new_num = formulas[wh_id](product)  # Apply the transformation formula for the current warehouse
                divisible, target_true, target_false = tests[wh_id](new_num)  # Determine the next warehouse based on divisibility test
                new_wh_id = target_true if divisible else target_false  # Choose the destination warehouse
                new_warehouses[new_wh_id].append(new_num)  # Assign the product to the new warehouse
                # Log the product movement
                results.append([wh_id, new_wh_id, product, new_num, divisible, day])

        # Update the warehouses dictionary for the next day's simulation
        warehouses = new_warehouses

    # Save results to CSV
    with open(data_dir / 'daily_operations.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Warehouse ID', 'Destination', 'Old Product ID', 'New Product ID', 'Test Result', 'Day'])
        writer.writerows(results)

if __name__ == '__main__':
    simulate()
