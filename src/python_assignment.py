import csv
from pathlib import Path

def read_initial_data(filename):
    warehouses = {}
    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()
            if line.startswith('Warehouse Unit:'):
                key = int(line.split(':')[-1].strip())
                warehouses[key] = {'products': [], 'name': f'Warehouse {key}', 'formula': '', 'test': ''}
            elif line.startswith('Starting Products:'):
                products = line.split(':')[-1].strip()
                warehouses[key]['products'] = [int(x) for x in products.split(',')]
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

formulas_text = {
    0: "multiply by 7",
    1: "square the number",
    2: "add 8",
    3: "add 4",
    4: "add 3",
    5: "add 5",
    6: "add 7",
    7: "multiply by 3",
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
        new_warehouses = {key: {'products': [], 'name': val['name']} for key, val in warehouses.items()}
        
        # Process each warehouse's products
        for wh_id, info in warehouses.items():
            for product in info['products']:
                new_product = formulas[wh_id](product)  # Apply the transformation formula for the current warehouse
                divisible, target_true, target_false = tests[wh_id](new_product)  # Determine the next warehouse based on divisibility test
                new_wh_id = target_true if divisible else target_false  # Choose the destination warehouse
                new_warehouses[new_wh_id]['products'].append(new_product)  # Assign the product to the new warehouse
                # Log the product movement
                results.append([
                    wh_id, 
                    info['name'], 
                    formulas_text[wh_id],
                    new_wh_id, 
                    product, 
                    new_product, 
                    divisible, 
                    day
                ])

        # Update the warehouses dictionary for the next day's simulation
        warehouses = new_warehouses

    # Save results to CSV
    with open(data_dir / 'daily_operations.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['warehouse_id', 'warehouse_name', 'warehouse_formula_text', 'product_destination', 'starting_product_id', 'current_product_id', 'test_bool', 'day_number'])
        writer.writerows(results)

if __name__ == '__main__':
    simulate()
