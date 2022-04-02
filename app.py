## IMPORTS
from google.cloud import firestore
from google.cloud.firestore_v1.query import Query
import json
from datetime import datetime, timedelta
import pytz
import streamlit as st
st.set_page_config(layout="wide")


## DB CONNECT
try:
    key_dict = json.loads(st.secrets["textkey"])
    db = firestore.Client.from_service_account_info(key_dict)
except Exception as e:
    st.write(f"Erro na conexão com o Banco de Dados:\n{e}")
    st.write("Se persistir o erro, contate o desenvolvedor!")




## FUNÇÕES
def __spaces(repeticoes: int = 3) -> None:
    _ = [st.write("") for _ in range(repeticoes)]



def __query(home: bool=False) -> Query.stream:
    """Busca no Banco de Dados com filtros.

    Args:
        home (bool, optional): Opção para identificar a busca específica para a Home. Defaults to False.

    Returns:
        Query.stream: Generator com objetos correspondentes aos filtros de busca aplicados.
    """
    fechamentos_ref = db.collection(u"teste_fechamento").document(u"JWEz12ARyHZGAs9qX3qy").collection("teste_")
    docs = fechamentos_ref.order_by(
        u"date", direction=firestore.Query.DESCENDING        
    ).limit(1)

    date_query = datetime.strptime(
        f"{docs.get()[0].to_dict()['date'].astimezone(pytz.timezone('America/Sao_Paulo')).date()}", "%Y-%m-%d"
    )
    shift = docs.get()[0].to_dict()["endedshift"]


    fechamentos_ref = db.collection(u"teste_fechamento")
    doc_ref = fechamentos_ref.where(
        u"date", u">", date_query
    ).where(
        u"date", u"<", date_query + timedelta(days=1)
    ).where(
        u"endedshift", u"==", f"{shift}"
    ).order_by(
        u"date", direction=firestore.Query.ASCENDING
    )

    return doc_ref.stream()




def __merge_docs(query: Query.stream) -> dict:
    """Concatena os documentos com a mesma especificação em um só.

    Args:
        query (Query.stream): Stream de queries resultantes da busca filtrada no banco de dados

    Returns:
        dict: Objeto com as informações concatenadas. 
        Retorna None caso não haja registros para a busca aplicada.
    """

    merged = {
        "date": "",
        "endedshift": "",
        "area": "",
        "coluna_di_saturada_100": "",
        "coluna_di_saturada_101": "",
        "regenerar_100": "",
        "regenerar_101": "",
        "troca_filtro_polidor_1": "",
        "troca_filtro_polidor_1": "",
    }

    for doc in query:
        for key in merged.keys():
            
            if key == "date":
                if doc.to_dict()[key] == "":
                    return None
                merged[key] = doc.to_dict()[key].astimezone(pytz.timezone("America/Sao_Paulo"))
            
            elif (key == "endedshift") | (merged[key] == ""):
                merged[key] = doc.to_dict()[key]   
            
            else:
                merged[key] = merged[key] + "\n\n" + doc.to_dict()[key]

    return merged


def __merge_teste(query: Query.stream) -> dict:
    """Concatena os documentos com a mesma especificação em um só.

    Args:
        query (Query.stream): Stream de queries resultantes da busca filtrada no banco de dados

    Returns:
        dict: Objeto com as informações concatenadas. 
        Retorna None caso não haja registros para a busca aplicada.
    """

    merged = {
        "date": "",
        "endedshift": "",
        "area": "",
        "coluna_di_saturada_100": "",
        "coluna_di_saturada_101": "",
        "regenerar_100": "",
        "regenerar_101": "",
        "troca_filtro_polidor_1": "",
        "troca_filtro_polidor_1": "",
    }

    for doc in query:
        for key in merged.keys():
            
            if key == "date":
                if doc.to_dict()[key] == "":
                    return None
                merged[key] = doc.to_dict()[key].astimezone(pytz.timezone("America/Sao_Paulo"))
            
            elif (key == "endedshift") | (merged[key] == ""):
                merged[key] = doc.to_dict()[key]   
            
            else:
                merged[key] = merged[key] + "\n\n" + doc.to_dict()[key]

    return merged





def __search_callback(home: bool) -> None:
    st.write(__merge_teste(__query()))





def __inserir_dados() -> None:
    """Estrutura de Formulário para inserir novos dados de turno para o Banco de Dados.
    """

    # Header
    st.header("Inserir Dados")

    col_shift, col_empty  = st.columns([1,5])
    with col_shift:
        st.selectbox(label="Turno: ", options=["Selecione", "A", "B", "C"], key="sft")
    with col_empty:
        st.empty()    
    
    # Forms
    with st.form(key='form_in', clear_on_submit=False):
        col_eta, col_eta_radio, col_empty1, col_mbr, col_mbr_radio, col_empty2, col_etei, col_etei_radio = st.columns([2,1,1,2,1,1,2,1])

        # Coluna da ETA
        with col_eta:
            st.header("ETA")
            __spaces(3)
            st.text("Troca do Filtro Polidor 1")
            __spaces(4)
            st.text("Troca do Filtro Polidor 2")   
            __spaces(3)
            st.text("Coluna DI Saturada 100")
            __spaces(4)
            st.text("Coluna DI Saturada 101")  
            __spaces(4)
            st.text("Necessário Regenerar 100")
            __spaces(4)
            st.text("Necessário Regenerar 101")  

        with col_eta_radio:      
            __spaces(5)   
            st.radio(label="", options=["Sim", "Não"], index=1, key="radio1")
            st.radio(label="", options=["Sim", "Não"], index=1, key="radio2")
            st.radio(label="", options=["Sim", "Não"], index=1, key="radio3")
            st.radio(label="", options=["Sim", "Não"], index=1, key="radio4")
            st.radio(label="", options=["Sim", "Não"], index=1, key="radio5")
            st.radio(label="", options=["Sim", "Não"], index=1, key="radio6")

        with col_empty1:
            st.empty()


        # Coluna do MBR
        with col_mbr:
            st.header("MBR")
            __spaces(3)
            st.text("Dosou Antiespumante")
            __spaces(4)
            st.text("Envio Sanitário MBR")   
            __spaces(3)
            st.text("Transbordou MBR") 

        with col_mbr_radio:      
            __spaces(5)   
            st.radio(label="", options=["Sim", "Não"], key="radio7")
            st.radio(label="", options=["Sim", "Não"], key="radio8")
            st.radio(label="", options=["Sim", "Não"], key="radio9")

        with col_empty2:
            st.empty()


        # Coluna da ETEI
        with col_etei:
            st.header("ETEI")
            __spaces(3)
            st.text("Troca do Filtro Polidor")
            __spaces(4)
            st.text("Nível Silo de Cal (%)")   

        with col_etei_radio:      
            __spaces(5)   
            st.radio(label="", options=["Sim", "Não"], key="radio10")
            st.text_input(label="", placeholder="0 - 100", key="silo_cal")



        # Observações
        __spaces(2)
        st.subheader("Observações")        
        
        col_obs_1, col_empty3, col_obs_2 = st.columns([6,1,6])

        # Coluna 1 Observações
        with col_obs_1:
            st.text_area("Geral", placeholder="Observações", key="obs_geral")

            __spaces(1)
            st.text_area("ETA", placeholder="Observações", key="obs_eta")

            __spaces(1)
            st.text_area("Produtos Químicos", placeholder="Observações", key="obs_quim")

        with col_empty3:
            st.empty()

        # Coluna 2 Observações
        with col_obs_2:
            st.text_area("ETEI", placeholder="Observações", key="obs_etei")

            __spaces(1)
            st.text_area("MBR / Aeração", placeholder="Observações", key="obs_mbr")

            __spaces(1)
            st.text_area("Sanitária", placeholder="Observações", key="obs_sanitaria")

        # Botão de Envio do Forms para o BD
        st.form_submit_button(label="Enviar")





def __buscar_dados() -> None:
    st.write("Tela para Buscar Dados")




def __submit_callback() -> None:
    pass




def main() -> None:
    """Função principal para guia de execuções na aplicação.
    """

    st.title("Diário de Turno - Meio Ambiente")

    # Side menu
    menu = ['Home', 'Inserir', 'Buscar']
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Home":
        __search_callback(home=True)
    elif choice == "Inserir":
        __inserir_dados()
    elif choice == "Buscar":
        __buscar_dados()



## EXECUÇÃO PRINCIPAL
if __name__ == "__main__":
    main()