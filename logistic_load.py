import pandas as pd
import geopandas as gpd
from datetime import datetime
from multiprocessing import Pool, cpu_count


# 물류 관련시설 운영시간 계산
def logistic_hours(db_path, usage):
    occupancy_file = f"{db_path}/inputs/technology/archetypes/use_types/{usage}.csv"
    occupancy = pd.read_csv(occupancy_file, header=2)
    result = {}
    for day in ['WEEKDAY', 'SATURDAY', 'SUNDAY']:
        day_df = occupancy[occupancy['DAY'] == day]
        result[day] = list(day_df['OCCUPANCY'])
    total_hours = len(result['WEEKDAY']) * 246 + len(result['SATURDAY']) * 52 + len(result['SUNDAY']) * 67
    return total_hours, result


# 운영시간에 따른 기기 가동 에너지 분배
def fill_logistic(row, usage, area, path, energy_per_sqm):
    if usage in ['COOL', 'COLD', 'GENERAL']:
        total_hours, result = logistic_hours(path, usage)
        week_day = result['WEEKDAY']
        saturday = result['SATURDAY']
        sunday = result['SUNDAY']
        day = datetime.fromisoformat(row['DATE']).weekday()
        hour = datetime.fromisoformat(row['DATE']).time().hour
        if day in range(5):
            return energy_per_sqm * area / total_hours * week_day[hour]

        elif day == 5:
            return energy_per_sqm * area / total_hours * saturday[hour]

        elif day == 6:
            return energy_per_sqm * area / total_hours * sunday[hour]
        else:
            return 0
    else:
        return 0


# 물류창고 평균 운송거리 * 평균 이용 트럭 용량을 토대로 필요 에너지량 계산
def fill_truck(row, monthly_package_loads, truck_ratio, truck_capacity, distance):
    annual_load = monthly_package_loads[row['1ST_USE']] * 12
    annual_truck_needed = annual_load / truck_capacity
    truck_needed_inout = truck_ratio * annual_truck_needed
    truck_needed = truck_needed_inout.transpose()
    truck_needed['sum'] = truck_needed['in'] * distance['in'] + truck_needed['out'] * distance['out']
    efficiency = pd.read_excel('logistic.xlsx', sheet_name='efficiency')
    efficiency.set_index('energy', inplace=True)
    print(efficiency)
    result = efficiency.copy()
    for i in efficiency.columns:
        result[i] = truck_needed.loc[i, 'sum'] / efficiency[i]
    result['sum'] = result.sum(axis=1)
    return result['sum'].to_dict()


# 물류창고 평균 운송거리 계산
def get_average_distance():
    data = pd.read_excel('logistic.xlsx', sheet_name='destination')
    data['sum'] = data.sum(axis=1, numeric_only=True)
    data['average_km'] = (18.33 * data['18.33km'] + 34.01 * data['34.01km'] + 195.51 * data['195.51km']) / data['sum']
    data.set_index('destination', inplace=True)
    return data['average_km'].to_dict()


# 물류창고 평균 운송 트럭 용량 계산
def get_average_weight():
    data = pd.read_excel('logistic.xlsx', sheet_name='monthly_cargos')
    data.set_index('destination', inplace=True)
    tot_weight = data.sum().sum()
    data = data / tot_weight
    result = data.transpose().to_dict()
    weighted_average = sum([i * (result['in'][i] + result['out'][i]) for i in data.columns])
    return data, weighted_average


# CEA 결과 파일 읽고 각각의 건물에 맞는 에너지값 계산 후 저장
def calculate_logistic_loads(item):
    path = item['path']
    name = item['Name']
    area = item['AREA']
    usage = item['1ST_USE']
    forklift = item['forklift']
    truck = item['truck']
    data_location = f"{path}/outputs/data/demand/{name}"
    data = pd.read_csv(data_location)
    data['forklift'] = data.apply(fill_logistic, axis=1, args=(usage, area, path, forklift))
    data['truck_diesel'] = data.apply(fill_logistic, axis=1, args=(usage, area, path, truck['diesel']))
    data['truck_kWh'] = data.apply(fill_logistic, axis=1, args=(usage, area, path, truck['kWh']))
    data.to_csv(data_location)


# CEA 입력값 확인 및 parsing
def get_building_info(db_path):
    architecture = gpd.read_file(f'{db_path}/inputs/building-properties/architecture.dbf')
    architecture.drop(columns=['geometry'], inplace=True)
    typology = gpd.read_file(f'{db_path}/inputs/building-properties/typology.dbf')
    typology.drop(columns=['geometry'], inplace=True)
    shape = gpd.read_file(f'{db_path}/inputs/building-geometry/zone.shp')
    building = shape.merge(typology, right_on='Name', left_on='Name')
    building = building.merge(architecture, right_on='Name', left_on='Name')
    building['AREA'] = building.area * building['floors_ag']
    building = building[['Name', 'AREA', '1ST_USE']]
    return building


# CEA 입력값 및 지게차, 트럭 specification에 따른 연간 예상 에너지 계산
# TODO energy 값 바꾸기
def get_logistic_loads(db_path):
    building_info = get_building_info(db_path)
    internal_loads = pd.read_excel(f"{db_path}/inputs/technology/archetypes/use_types/USE_TYPE_PROPERTIES.xlsx",
                                   sheet_name='INTERNAL_LOADS')
    internal_loads.set_index('code', inplace=True)
    internal_loads_dict = internal_loads.to_dict()
    monthly_package_loads = internal_loads_dict[f'monthly_package_ton']
    forklift_capacity = internal_loads_dict[f'forklift_capacity_ton']
    energy = 48 * 400 / 1000
    building_info['forklift'] = building_info.apply(
        lambda x: energy * monthly_package_loads[x['1ST_USE']] * 12 / forklift_capacity[x['1ST_USE']], axis=1)
    truck_needed, weight = get_average_weight()
    distance = get_average_distance()
    building_info['truck'] = building_info.apply(fill_truck, axis=1, args=(monthly_package_loads,
                                                                           truck_needed, weight, distance))
    building_info = building_info[['Name', 'AREA', 'forklift', 'truck', '1ST_USE']]
    return building_info


# multiprocessing 여부에 따른 작업
def process_logistic_loads(db_path, multi_processing=True):
    building_info = get_logistic_loads(db_path)
    building_info['path'] = db_path
    data = list(building_info.transpose().to_dict())
    if multi_processing:
        multi = cpu_count() - 1
        Pool(multi).map(calculate_logistic_loads, data)
    else:
        for datum in data:
            calculate_logistic_loads(datum)


def main():
    db_path = input("Please input CEA scenario path: ")
    multi = input("Are you going to run multiprocessing? (y/n) : ")
    if multi == 'y':
        process_logistic_loads(db_path)
    elif multi == 'n':
        process_logistic_loads(db_path, multi_processing=False)


if __name__ == '__main__':
    main()
