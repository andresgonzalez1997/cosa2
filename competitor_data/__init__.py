import competitor_data.purina_file_horizontal as purina

def get_purina_price_list(file_path):
    return purina.read_file(file_path)

def get_purina_location(file_path):
    return purina.plant_location(file_path)

def get_purina_effective_date(file_path):
    return purina.effective_date(file_path)