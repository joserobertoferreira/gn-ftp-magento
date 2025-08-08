import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, Optional

import schedule


class Scheduler:
    def __init__(self, job_function: Callable, config: Dict[str, Any], post_job_function: Optional[Callable] = None):
        self.job_function = job_function
        self.config = config
        self.post_job_function = post_job_function
        self.post_job_done: Optional[date] = None
        self.window_closed_timestamp: Optional[datetime] = None
        self.logger = logging.getLogger(__name__)

    def is_within_time_window(self) -> bool:
        """Verifica se o horário atual está dentro da janela permitida"""
        now = datetime.now().time()
        start = datetime.strptime(self.config['SCHEDULE_START_TIME'], '%H:%M').time()
        end = datetime.strptime(self.config['SCHEDULE_END_TIME'], '%H:%M').time()

        if start <= end:
            return start <= now <= end
        else:
            # Caso a janela passe da meia-noite
            return now >= start or now <= end

    def is_allowed_month(self) -> bool:
        """Verifica se o mês atual está na lista de meses permitidos"""
        current_month = datetime.now().month
        return current_month in self.config['SCHEDULE_MONTHS']

    def should_run(self) -> bool:
        """Determina se a execução deve ocorrer com base nas regras"""
        if not self.config['SCHEDULE_ENABLED']:
            return False

        if not self.is_allowed_month():
            return False

        if not self.is_within_time_window():
            return False

        return True

    def scheduled_job(self):
        """Wrapper que verifica as condições antes de executar"""
        if self.should_run():
            try:
                self.logger.info('Iniciando execução agendada...')
                self.job_function()
            except Exception as e:
                self.logger.error(f'Erro ao executar o job principal: {e}', exc_info=True)
        else:
            self.logger.debug('Execução não permitida no momento (configuração ou horário inválido)')

    def _handle_post_execution(self):
        """
        Verifica se a janela de execução terminou e executa a tarefa de
        pós-execução se necessário.
        """
        # Se a funcionalidade não foi configurada, não faz nada
        if not self.post_job_function:
            return

        today = datetime.now().date()

        # Se a tarefa de finalização já foi executada hoje, não faz nada
        if self.post_job_done == today:
            self.window_closed_timestamp = None
            return

        # Condições básicas: Agendamento ativo e mês permitido
        if not self.config['SCHEDULE_ENABLED'] or not self.is_allowed_month():
            return

        is_in_window = self.is_within_time_window()

        if is_in_window:
            # Se estamos dentro da janela, garantimos que o "cronômetro" de delay está zerado.
            # Isso é importante para o início de um novo ciclo no dia seguinte.
            if self.window_closed_timestamp:
                self.logger.debug('Janela de execução reaberta. Inicializar timer de pós-execução.')
                self.window_closed_timestamp = None
            # E inicializa a flag do dia, caso o dia tenha virado.
            if self.post_job_done != today:
                self.post_job_done = None
            return

        # Cenário 1: Acabamos de sair da janela. Inicia o cronômetro.
        if self.window_closed_timestamp is None:
            self.logger.info('Janela de execução fechada. Iniciar contagem para a tarefa de pós-execução.')
            self.window_closed_timestamp = datetime.now()
            return

        # Cenário 2: O cronômetro já foi iniciado. Verificamos se o tempo de delay passou.
        delay_minutes = self.config.get('POST_EXECUTION_DELAY_MINUTES', 60)
        time_since_closure = datetime.now() - self.window_closed_timestamp

        if time_since_closure >= timedelta(minutes=delay_minutes):
            self.logger.info(f'Delay de {delay_minutes} minutos concluído. Executar tarefa de pós-execução...')
            try:
                self.post_job_function()
                # Marca a tarefa como concluída para hoje para não rodar de novo
                self.post_job_done = today
                self.logger.info('Tarefa de pós-execução concluída com sucesso.')
            except Exception as e:
                self.logger.error(f'Erro ao executar a tarefa de pós-execução: {e}', exc_info=True)
                self.window_closed_timestamp = None

    def start(self):
        """Inicia o scheduler com as configurações definidas"""
        if not self.config['SCHEDULE_ENABLED']:
            self.logger.info('Agendamento desativado nas configurações')
            return

        if self.config['SCHEDULE_RUN_IMMEDIATELY'] and self.should_run():
            self.logger.info('Executando primeira sincronização imediatamente...')
            self.job_function()

        # Agenda a execução periódica
        schedule.every(self.config['SCHEDULE_INTERVAL_MINUTES']).minutes.do(self.scheduled_job)

        self.logger.info(f"""Agendamento configurado:
        - Meses permitidos: {self.config['SCHEDULE_MONTHS']}
        - Janela de execução: {self.config['SCHEDULE_START_TIME']} às {self.config['SCHEDULE_END_TIME']}
        - Intervalo: {self.config['SCHEDULE_INTERVAL_MINUTES']} minutos
        - Agendamento {'ativo' if self.config['SCHEDULE_ENABLED'] else 'inativo'}
        - Tarefa de pós-execução: {'configurada' if self.post_job_function else 'não configurada'}
        - Delay da tarefa final: {self.config.get('POST_EXECUTION_DELAY_MINUTES', 60)} minutos""")

        try:
            interval = self.config['SCHEDULE_INTERVAL_MINUTES']

            while True:
                schedule.run_pending()
                self._handle_post_execution()
                time.sleep(interval * 60)
        except KeyboardInterrupt:
            self.logger.info('Recebido sinal de interrupção. Encerrando...')
        except Exception as e:
            self.logger.error(f'Erro inesperado no scheduler: {e}')
            raise
