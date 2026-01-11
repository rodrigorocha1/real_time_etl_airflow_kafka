# hooks/sptrans_hook.py
import requests
from airflow.hooks.base import BaseHook


class SPTransHook(BaseHook):
    """
    Hook que autentica e busca posições na SPTrans
    """

    def __init__(self, token: str):
        super().__init__()
        self.token = token

    def autenticar(self):
        url = f"https://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token={self.token}"
        response = requests.post(url)
        response.raise_for_status()
        cookie = response.headers.get("Set-Cookie")
        if not cookie:
            raise ValueError("Não foi possível obter o cookie de autenticação")
        return cookie

    def obter_posicoes(self):
        cookie = self.autenticar()
        url = "http://api.olhovivo.sptrans.com.br/v2.1/Posicao"
        headers = {'Cookie': cookie}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
