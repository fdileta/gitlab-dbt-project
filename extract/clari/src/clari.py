from dateutil import parser as date_parser
from os import environ as env

config_dict = env.copy()


def _calc_fiscal_quarter(dt):
    fiscal_year = dt.year + 1
    if dt.month in [2, 3, 4]:
        fiscal_quarter = 1
    elif dt.month in [5, 6, 7]:
        fiscal_quarter = 2
    elif dt.month in [8, 9, 10]:
        fiscal_quarter = 3
    else:
        fiscal_quarter = 4

    # Format the fiscal year and quarter as a string
    fiscal_year_quarter = f'{fiscal_year}_Q{fiscal_quarter}'
    return fiscal_year_quarter


def _get_previous_fiscal_quarter(dt):
    current_fiscal_quarter = _calc_fiscal_quarter(dt)
    fiscal_quarter_prefix = current_fiscal_quarter[:-1]
    current_quarter_int = int(current_fiscal_quarter[-1])

    if current_quarter_int == 1:
        return fiscal_quarter_prefix + '4'
    return fiscal_quarter_prefix + f'{current_quarter_int - 1}'


def get_fiscal_quarter():
    execution_date = date_parser.parser(config_dict['execution_date'])
    task_schedule = config_dict['task_schedule']

    print(
        f'Calculating quarter based on the following task_schedule and execution_date: {task_schedule} | {execution_date}'
    )

    if task_schedule == 'daily':
        return _calc_fiscal_quarter(execution_date)

    # else quarterly task schedule
    return _get_previous_fiscal_quarter(execution_date)


def main():
    fiscal_quarter = get_fiscal_quarter()
    print(f'\nfiscal_quarter: {fiscal_quarter}')


if __name__ == '__main__':
    main()
