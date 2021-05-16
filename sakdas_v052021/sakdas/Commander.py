#For Prod
#from sakdas.Analyst import Analyst
#from sakdas.Selector import Selector
#from sakdas.Auditor import Auditor
#from sakdas.Visualizer import Visualizer

#For Intenal Test
import sys
sys.path.append('/Users/sakdaloetpipatwanich/Desktop/SAKDAS/sakdas_dm/sakdas_v052021/sakdas')
#from Commander import Commander
from Analyst import Analyst
from Selector import Selector
from Auditor import Auditor
from Visualizer import Visualizer

class Commander:
    def __init__(self, sakda_order):
        self.order = sakda_order
        
    def action(self):
        def order_review():
            #print('Passed')
            return True


        order_review()
        #self.selected_data_engine   = Selector(self.order.data_engine).task_submit()
        self.data_profiling_result  = Analyst(self.order.df, self.order.df_name, self.order.path_to_output).task_submit()
        print(self.data_profiling_result)
        self.data_auditing_result   = Auditor(self.data_profiling_result, self.order.auditing_config, self.order.path_to_output).task_submit()
        self.Visualizing_result     = Visualizer(self.data_profiling_result, self.data_auditing_result, self.order.path_to_output, self.order.report_config, self.order.auditing_config).task_submit()


        result = {}
        result['data_profile']  = self.data_profiling_result
        result['data_auditing'] = self.data_auditing_result
        result['data_viz']      = self.Visualizing_result


        return result

