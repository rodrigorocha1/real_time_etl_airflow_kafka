from typing import Any, Dict

from airflow.models import BaseOperator, Variable
from hooks.sptrans_hook import SPTransHook


class SPTransPosicaoOperator(BaseOperator):
    """
    Operator que autentica na SPTrans e busca posições de ônibus.

    :param token: Token da SPTrans para autenticação
    :param http_conn_id: Conexão HTTP cadastrada no Airflow
    """

    template_fields = ("token",)

    def __init__(self,  http_conn_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = Variable.get('CHAVE_SPTRANS')
        self.http_conn_id = http_conn_id

    def execute(self, context: Dict[str, Any]):
        self.log.info("Iniciando busca de posições de ônibus na SPTrans...")
        hook = SPTransHook(token=self.token, http_conn_id=self.http_conn_id)
        posicoes = hook.obter_posicoes()
        total_onibus = len(posicoes) if isinstance(posicoes, list) else 0
        self.log.info(f"Total de ônibus encontrados: {total_onibus}")

        return posicoes
