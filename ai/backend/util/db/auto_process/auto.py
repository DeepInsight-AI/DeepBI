from ai.backend.util.db.auto_process.create_new_sp_ad_auto import Ceate_new_sku

def auto_process():
    api = Ceate_new_sku()
    #api.create_new_sp_auto_no_template1('US',['L59', 'M121', 'M23', 'M118', 'M108', 'G11', 'L96', 'M103', 'L103', 'M82', 'M131'],'LAPASA')
    api.create_new_sp_auto_no_template('US',['B0D4MBL27S'],'BDZX_ZEN_CAVE')
    #api.create_new_sp_manual_no_template('US',['M100','M118'],'LAPASA')

auto_process()

