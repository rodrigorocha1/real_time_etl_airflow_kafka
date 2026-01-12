from airflow.providers.http.hooks.http import HttpHook


class SPTransHook(HttpHook):
    """
    Hook que autentica e busca posições na SPTrans usando HttpHook do Airflow.
    """

    def __init__(self, token: str, http_conn_id: str, *args, **kwargs):
        """
        :param token: token da SPTrans
        :param http_conn_id: ID da conexão HTTP cadastrada no Airflow
        """
        super().__init__(http_conn_id=http_conn_id, method="POST", *args, **kwargs)
        self.token = token

    def autenticar(self) -> str:
        """
        Autentica no SPTrans e retorna o cookie da sessão
        """
        endpoint = f"/Login/Autenticar?token={self.token}"
        response = self.run(endpoint)
        print(response)  # POST é o default do hook
        cookie = response.headers.get("Set-Cookie")
        print(f"Cookie recebido: {cookie}")
        if not cookie:
            raise ValueError("Não foi possível obter o cookie de autenticação")
        return cookie

    def obter_posicoes(self) -> dict:
        """
        Busca posições de ônibus da SPTrans
        """
        try:
            cookie = self.autenticar()
            headers = {"Cookie": cookie}
            self.method = "GET"

            # No Airflow 3.x, sobrescrevemos o método HTTP usando http_method
            response = self.run("/Posicao", headers=headers, )
            print(f"Resposta da API de posições: {response.text}")
            return response.json()
        except Exception as e:
            print(f"Erro ao obter posições: {e}")
            return {}
