# operators/sptrans_operators.py
from airflow.models import BaseOperator
from hooks.sptrans_hook import SPTransHook


class SPTransPosicaoOperator(BaseOperator):
    """
    Operator que autentica na SPTrans e busca posições de ônibus
    """

    def __init__(self, token: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = token

    def execute(self, context):
        hook = SPTransHook(token=self.token)
        posicoes = hook.obter_posicoes()  # Faz login e busca posições
        self.log.info(f"Total de ônibus: {len(posicoes)}")
        print(f"posicoes: {posicoes}")
        return posicoes
