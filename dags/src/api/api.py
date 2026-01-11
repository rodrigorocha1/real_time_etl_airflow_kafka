from typing import Tuple

import requests
from airflow.models import Variable


class API:
    @classmethod
    def fazer_login_api(cls) -> Tuple[str, bool]:
        """Método para executar o login na API da sptran

        Returns:
            Tuple[str, bool]: Uma tupla com mensagem de erro ou cookie e um booleano
        """
        try:
            auth = requests.post(
                Variable.get(
                    'URL_SPTRANS') + '/Login/Autenticar?token=' + Variable.get('CHAVE_SPTRANS')
            )

            credenciais = auth.headers["Set-Cookie"].split(";")[
                0].split("=")[-1]
            return credenciais, True
        except requests.exceptions.ConnectTimeout:
            return 'Timeout no login', False
        except requests.exceptions.ConnectionError:
            return 'Erro na conexão', False
