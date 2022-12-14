# Copyright @ 2020 Thought Machine Group Limited. All rights reserved.
api = "3.9.0"
version = "0.0.1"
display_name = "CASA"
summary = "Savings Account "
tside = Tside.LIABILITY
supported_denominations = 'PHP'
MONTHLY_MAINTENANCE_FEES = 'MONTHLY_MAINTENANCE_FEES'
ACCRUED_INTEREST = 'ACCRUED_INCOMING_INTEREST'

parameters = [
    #Template Params
    Parameter(
        name='denomination',
        shape=DenominationShape,
        level=Level.TEMPLATE,
        description='Default denomination for the contract.',
        display_name='Default Denomination',
        update_permission=UpdatePermission.FIXED,
    ),
    Parameter(
        name='fee_tiers',
        shape=StringShape,
        level=Level.TEMPLATE,
        description='The monthly fee rate for this account',
        display_name='The monthly fee rate for this account',
    ),
    Parameter(
        name='fee_tier_ranges',
        shape=StringShape,
        level=Level.TEMPLATE,
        description='The available fee tiers',
        display_name='The available fee tiers',
    ),
    Parameter(
        name='internal_account',
        shape=AccountIdShape,
        level=Level.TEMPLATE,
        description='Internal account ID.',
        display_name='Internal account ID',
    ),
    #Instance Params
    Parameter(
        name="base_interest_rate",
        shape=NumberShape(
            kind=NumberKind.PERCENTAGE
        ),
        level=Level.INSTANCE,
        description="Base interest rate given by the bank to the account holder.",
        display_name="Base Interest Rate",
        update_permission=UpdatePermission.OPS_EDITABLE,
        default_value=Decimal(2)
    ),
    Parameter(
        name="bonus_interest_rate",
        shape=NumberShape(
            kind=NumberKind.PERCENTAGE
        ),
        level=Level.INSTANCE,
        description="Bonus interest rate given by the bank to the account holder.",
        display_name="Bonus Interest Rate",
        update_permission=UpdatePermission.OPS_EDITABLE,
        default_value=Decimal(2)
    ),
    Parameter(
        name="bonus_interest_amount_threshold",
        level=Level.INSTANCE,
        description="Amount threshold for the Bonus Interest to be applied.",
        display_name="Amount Treshold for Bonus Interest",
        update_permission=UpdatePermission.OPS_EDITABLE,
        shape=NumberShape(
            kind=NumberKind.MONEY
        ),
        default_value=Decimal(1000)
    ),
    Parameter(
        name="minimum_balance_maintenance_fee_waive",
        level=Level.INSTANCE,
        description="Minimum balance for the monthly maintenance fee to be waived.",
        display_name="Minimum balance for the monthly maintenance fee to be waived",
        update_permission=UpdatePermission.OPS_EDITABLE,
        shape=NumberShape(
            kind=NumberKind.MONEY
        ),
        default_value=Decimal(1000)
    ),
    Parameter(
        name="flat_fee",
        level=Level.INSTANCE,
        description="Flat fee to be applied.",
        display_name="Flat Fee to be applied",
        update_permission=UpdatePermission.USER_EDITABLE,
        shape=NumberShape(
            kind=NumberKind.MONEY
        ),
        default_value="0"
    )
]

@requires(parameters=True)
def execution_schedules():
    creation_date = vault.get_account_creation_date()
    return [
        (
            'DAILY_ACCRUE_INTEREST',
            {
                'hour': '0',
                'minute': '0',
                'second': '1'
            }
        ),
        (
            'DAILY_APPLY_INTEREST',
            {
                'hour': '0',
                'minute': '0',
                'second': '5',
            }
        ),
        (
            'MONTHLY_MAINTENANCE_FEE',
            {
                'day': str(creation_date.day),
                'hour': '0',
                'minute': '0',
                'second': '0',
                'start_date': str((creation_date + timedelta(months=1)).date())
            }
        )
    ]

@requires(event_type='DAILY_ACCRUE_INTEREST', parameters=True, balances="1 day")
@requires(event_type='DAILY_APPLY_INTEREST', parameters=True, balances="1 day")
@requires(event_type='MONTHLY_MAINTENANCE_FEE', parameters=True, balances="1 day")
def scheduled_code(event_type, effective_date):
    denomination = vault.get_parameter_timeseries(name='denomination').latest()
    internal_account = vault.get_parameter_timeseries(name='internal_account').latest()

    if event_type == 'DAILY_ACCRUE_INTEREST':
        balances = vault.get_balance_timeseries().before(timestamp=effective_date)
        _accrue_interest(
            vault, denomination, internal_account, effective_date, balances
        )

    elif event_type == 'DAILY_APPLY_INTEREST':
        balances = vault.get_balance_timeseries().latest()
        _apply_interest(
            vault, denomination, internal_account, effective_date, balances
        )

    elif event_type == 'MONTHLY_MAINTENANCE_FEE':
        balances = vault.get_balance_timeseries().latest()
        _apply_maintenance_fee(
            vault, denomination, internal_account, effective_date, balances
        )

@requires(parameters=True, balances='latest', postings='1 day',)
def pre_posting_code(postings, effective_date):
    denomination = vault.get_parameter_timeseries(name='denomination').latest()

    if any(post.denomination != denomination for post in postings):
        raise Rejected(
            'Cannot make transaction in given denominations; '
            'transactions must be in {}'.format(supported_denominations),
            reason_code=RejectedReason.WRONG_DENOMINATION,
            )

    balances = vault.get_balance_timeseries().latest()
    
    for post in postings: 
        if post.credit == False: 
            proposed_amount = sum(
                post.amount for post in postings if post.account_address == DEFAULT_ADDRESS
                and post.asset == DEFAULT_ASSET
            )

            total_balances = sum(
                balance.net for ((address, asset, denomination, phase), balance) in balances.items() if
                address == DEFAULT_ADDRESS
            )

            #check for checking withdrawal amount is not greater than the balance
            if proposed_amount > total_balances:
                raise Rejected(
                    'Cannot withdraw proposed amount.', 
                    reason_code=RejectedReason.INSUFFICIENT_FUNDS
                    )
            else:
                None

def _accrue_interest(vault, denomination, internal_account, effective_date, balances):
    hook_execution_id = vault.get_hook_execution_id()
    effective_balance = balances[
        (DEFAULT_ADDRESS, DEFAULT_ASSET, denomination, Phase.COMMITTED)
    ].net
    daily_rate = vault.get_parameter_timeseries(name='base_interest_rate').latest()
    interest = effective_balance * _apply_interest_with_bonus(vault, effective_balance, daily_rate)
    amount_to_accrue = _precision_accrual(interest)

    if amount_to_accrue > 0:
        posting_ins = vault.make_internal_transfer_instructions(
            amount=amount_to_accrue,
            denomination=denomination,
            client_transaction_id=hook_execution_id + '_DAILY_INTEREST_ACCRUAL',
            from_account_id=internal_account,
            from_account_address='ACCRUED_OUTGOING',
            to_account_id=vault.account_id,
            to_account_address='ACCRUED_INCOMING_INTEREST',
            instruction_details={
                'description': f'Daily interest accrued at {daily_rate} on balance '
                               f'of {effective_balance}',
                'event': 'ACCRUE_INTEREST'
            },
            asset=DEFAULT_ASSET
        )
        vault.instruct_posting_batch(
            posting_instructions=posting_ins, effective_date=effective_date
        )

def _apply_interest_with_bonus(vault, effective_balance, interest):
    bonus_interest_amount_threshold = vault.get_parameter_timeseries(name="bonus_interest_amount_threshold").latest()

    if effective_balance > bonus_interest_amount_threshold:
        bonus_interest = vault.get_parameter_timeseries(name="bonus_interest_rate").latest()
        interest += bonus_interest

    return interest 

def _apply_interest(vault, denomination, internal_account, effective_date, balances):
    hook_execution_id = vault.get_hook_execution_id()
    interest_accrued = balances[
        ('ACCRUED_INCOMING_INTEREST', DEFAULT_ASSET, denomination, Phase.COMMITTED)
    ].net

    if interest_accrued > 0:
        posting_ins = vault.make_internal_transfer_instructions(
                        amount=interest_accrued,
                        denomination=denomination,
                        from_account_id=vault.account_id,
                        from_account_address='ACCRUED_INCOMING_INTEREST',
                        to_account_id=vault.account_id,
                        to_account_address=DEFAULT_ADDRESS,
                        asset=DEFAULT_ASSET,
                        client_transaction_id=f'APPLY_ACCRUED_INTEREST'
                                            f'{hook_execution_id}_{denomination}',
                        instruction_details={
                            'description': 'Interest Applied',
                            'event': 'APPLY_ACCRUED_INTEREST'
                        }
                    )

        posting_ins.extend(
            vault.make_internal_transfer_instructions(
                amount= interest_accrued, 
                denomination=denomination,
                from_account_id=internal_account,
                from_account_address=DEFAULT_ADDRESS,
                to_account_id=vault.account_id,
                to_account_address='ACCRUED_INTEREST',
                asset=DEFAULT_ASSET,
                client_transaction_id=f'APPLY_ACCRUED_INTEREST_{hook_execution_id}_'
                                    f'{denomination}_CUSTOMER',
                instruction_details={
                    'description': 'Interest Applied',
                    'event': 'APPLY_ACCRUED_INTEREST'
                }
            )
        )
        vault.instruct_posting_batch(
                posting_instructions=posting_ins,
                effective_date=effective_date,
                client_batch_id=f'APPLY_ACCRUED_INTEREST{hook_execution_id}_'
                                f'{denomination}'
            )

def _precision_accrual(amount):
    return amount.copy_abs().quantize(Decimal('.0001'), rounding=ROUND_HALF_UP)

def _apply_maintenance_fee(vault, denomination, internal_account, effective_date, balances):
    hook_execution_id = vault.get_hook_execution_id()
    effective_balance = balances[
        (DEFAULT_ADDRESS, DEFAULT_ASSET, denomination, Phase.COMMITTED)
    ].net
    minimum_balance_maintenance_fee_waive = vault.get_parameter_timeseries(name="minimum_balance_maintenance_fee_waive").latest()

    if _monthly_mean_balance(vault, denomination, effective_date) < minimum_balance_maintenance_fee_waive: 
        flat_fee = vault.get_parameter_timeseries(name="flat_fee").latest()

        if flat_fee > 0:
            posting_ins = vault.make_internal_transfer_instructions(
                        amount=flat_fee,
                        denomination=denomination,
                        from_account_id=vault.account_id,
                        from_account_address=MONTHLY_MAINTENANCE_FEES,
                        to_account_id=internal_account,
                        to_account_address='MONTHLY_FEE_ACCRUED',
                        asset=DEFAULT_ASSET,
                        client_transaction_id=f'APPLY_FEE'
                                            f'{hook_execution_id}_{denomination}',
                        instruction_details={
                            'description': f'Monthly Maintenance Fee Applied to Account: {vault.account_id}' ,
                            'event': 'APPLY_MONTHLY_FEE'
                        }
                    )

            posting_ins.extend(
                vault.make_internal_transfer_instructions(
                    amount=flat_fee,
                    denomination=denomination,
                    from_account_id=vault.account_id,
                    from_account_address=DEFAULT_ADDRESS,
                    to_account_id=internal_account,
                    to_account_address=DEFAULT_ADDRESS  ,
                    asset=DEFAULT_ASSET,
                    client_transaction_id=f'APPLY_FEE'
                                        f'{hook_execution_id}_{denomination}_INTERNAL',
                    instruction_details={
                        'description': f'Monthly Maintenance Fee Applied to Account: {vault.account_id}' ,
                        'event': 'APPLY_MONTHLY_FEE'
                    }
                )
            )

            vault.instruct_posting_batch(
                posting_instructions=posting_ins,
                effective_date=effective_date,
                client_batch_id=f'APPLY_MONTHLY_FEE{hook_execution_id}_')
                 
        else: 
            fee_tiers = json_loads(vault.get_parameter_timeseries(name="fee_tiers").latest())
            fee_tier_ranges = json_loads(vault.get_parameter_timeseries(name="fee_tier_ranges").latest())

            applicable_monthly_fee =_get_tiered_monthly_fee (effective_balance, fee_tiers, fee_tier_ranges)

            if applicable_monthly_fee > 0:
                posting_ins = vault.make_internal_transfer_instructions(
                            amount=applicable_monthly_fee,
                            denomination=denomination,
                            from_account_id=vault.account_id,
                            from_account_address=MONTHLY_MAINTENANCE_FEES,
                            to_account_id=internal_account,
                            to_account_address='MONTHLY_MAINTENANCE_FEE_ACCRUED',
                            asset=DEFAULT_ASSET,
                            client_transaction_id=f'APPLY_FEE'
                                                f'{hook_execution_id}_{denomination}',
                            instruction_details={
                                'description': f'Monthly Maintenance Fee Applied to Account: {vault.account_id}' ,
                                'event': 'APPLY_MONTHLY_FEE'
                            }
                        )

                posting_ins.extend(
                    vault.make_internal_transfer_instructions(
                        amount=applicable_monthly_fee,
                        denomination=denomination,
                        from_account_id=vault.account_id,
                        from_account_address=DEFAULT_ADDRESS,
                        to_account_id=internal_account,
                        to_account_address=DEFAULT_ADDRESS  ,
                        asset=DEFAULT_ASSET,
                        client_transaction_id=f'APPLY_FEE'
                                            f'{hook_execution_id}_{denomination}_INTERNAL',
                        instruction_details={
                            'description': f'Monthly Maintenance Fee Applied to Account: {vault.account_id}' ,
                            'event': 'APPLY_MONTHLY_FEE'
                        }
                    )
                )

                vault.instruct_posting_batch(
                    posting_instructions=posting_ins,
                    effective_date=effective_date,
                    client_batch_id=f'APPLY_MONTHLY_FEE{hook_execution_id}_')
    else:
        return

def _get_tiered_monthly_fee(effective_balance, fee_tiers, fee_tier_ranges):
    tier = None
    for fee_tier in fee_tier_ranges: 
        bounds = fee_tier_ranges[fee_tier]
        if bounds['min'] <= effective_balance <= bounds['max']:
            tier = fee_tier
    
    if tier: 
        fee = Decimal(fee_tiers[tier])
    else: 
        fee = 0

    return fee

def _monthly_mean_balance(vault, denomination, effective_date):
    creation_date = vault.get_account_creation_date()
    period_start = effective_date - timedelta(months=1)
    if period_start < creation_date:
        period_start += timedelta(days=1)
    num_days = (effective_date - period_start).days
    total = sum(
        [
            vault.get_balance_timeseries()
            .at(timestamp=period_start + timedelta(days=i))[
                (DEFAULT_ADDRESS, DEFAULT_ASSET, denomination, Phase.COMMITTED)
            ]
            .net
            for i in range(num_days)
        ]
    )
    mean_balance = total / num_days
    return mean_balance