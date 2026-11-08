import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from datetime import time as dt_time
from typing import Any, Callable, Dict, List

MINUTES_PER_HOUR = 60
MINUTES_PER_DAY = 24 * MINUTES_PER_HOUR

# Tempo máximo de espera entre verificações, para o ciclo se manter reativo mesmo sem execuções próximas
MAX_SLEEP_SECONDS = 30


def _parse_time(value: str) -> dt_time:
    return datetime.strptime(value, '%H:%M').time()


class TaskSchedule:
    """
    Horas de execução diária de uma tarefa, alinhadas ao relógio.

    As execuções ocorrem em `start`, `start + interval`, ... até `end` (inclusive). Se `end` for
    inferior a `start`, a janela atravessa a meia-noite. Se `start` e `end` forem iguais, há uma
    única execução por dia.
    """

    def __init__(self, start: str, end: str, interval_minutes: int):
        if interval_minutes <= 0:
            raise ValueError(f'O intervalo tem de ser superior a zero (recebido: {interval_minutes}).')

        self.start = _parse_time(start)
        self.end = _parse_time(end)
        self.interval_minutes = interval_minutes
        self.slots = self._build_slots()

    def _build_slots(self) -> List[dt_time]:
        start_minute = self.start.hour * MINUTES_PER_HOUR + self.start.minute
        end_minute = self.end.hour * MINUTES_PER_HOUR + self.end.minute

        if end_minute < start_minute:
            end_minute += MINUTES_PER_DAY

        minutes = {m % MINUTES_PER_DAY for m in range(start_minute, end_minute + 1, self.interval_minutes)}
        return [dt_time(m // MINUTES_PER_HOUR, m % MINUTES_PER_HOUR) for m in sorted(minutes)]

    def next_after(self, moment: datetime) -> datetime:
        """Devolve a primeira execução prevista estritamente depois de `moment`."""
        days = (moment.date(), moment.date() + timedelta(days=1))
        candidates = (datetime.combine(day, slot) for day in days for slot in self.slots)
        return next(candidate for candidate in candidates if candidate > moment)

    def has_slot_at(self, moment: datetime) -> bool:
        """Indica se existe uma execução prevista para o minuto de `moment`."""
        return moment.time().replace(second=0, microsecond=0) in self.slots

    def is_within_window(self, moment: datetime) -> bool:
        """Indica se `moment` está dentro da janela de execução."""
        current = moment.time()

        if self.start <= self.end:
            return self.start <= current <= self.end

        # Caso a janela passe da meia-noite
        return current >= self.start or current <= self.end


@dataclass
class ScheduledTask:
    """Uma rotina e o respetivo horário."""

    name: str
    function: Callable[[], Any]
    schedule: TaskSchedule
    # Em execução única, corre sempre, mesmo sem horário previsto para o minuto atual
    always_in_single_run: bool = False
    # Valor neutro até o ciclo do Scheduler planear a primeira execução
    next_run: datetime = datetime.max


class Scheduler:
    def __init__(self, tasks: List[ScheduledTask], config: Dict[str, Any]):
        self.tasks = tasks
        self.config = config
        self.logger = logging.getLogger(__name__)

    def is_allowed_month(self) -> bool:
        """Verifica se o mês atual está na lista de meses permitidos"""
        return datetime.now().month in self.config['SCHEDULE_MONTHS']

    def _execute(self, task: ScheduledTask) -> None:
        """Executa uma tarefa, isolando as falhas para não afetar as restantes"""
        self.logger.info(f"Iniciar tarefa '{task.name}'...")
        try:
            if task.function() is False:
                self.logger.warning(f"A tarefa '{task.name}' terminou sem sucesso.")
        except Exception as e:
            self.logger.error(f"Erro ao executar a tarefa '{task.name}': {e}", exc_info=True)

    def _execute_scheduled(self, task: ScheduledTask) -> None:
        """Executa a tarefa, se o mês o permitir, e planeia a execução seguinte"""
        if self.is_allowed_month():
            self._execute(task)
        else:
            self.logger.debug(f"Tarefa '{task.name}' ignorada: mês não permitido.")

        # Calculado depois da execução: horários ultrapassados por uma tarefa demorada são ignorados
        task.next_run = task.schedule.next_after(datetime.now())

    def _run_due_tasks(self) -> None:
        for task in self.tasks:
            if task.next_run <= datetime.now():
                self._execute_scheduled(task)

    def _seconds_until_next_run(self) -> float:
        next_run = min(task.next_run for task in self.tasks)
        return min(max((next_run - datetime.now()).total_seconds(), 0), MAX_SLEEP_SECONDS)

    def _log_configuration(self) -> None:
        lines = [
            f'- {task.name}: {task.schedule.start:%H:%M} às {task.schedule.end:%H:%M}, '
            f'a cada {task.schedule.interval_minutes} minutos (próxima: {task.next_run:%d/%m %H:%M})'
            for task in self.tasks
        ]
        self.logger.info(
            f'Agendamento configurado (meses permitidos: {self.config["SCHEDULE_MONTHS"]}):\n' + '\n'.join(lines)
        )

    def run_once(self) -> None:
        """
        Execução única: corre as tarefas com horário previsto para o minuto atual e as
        marcadas com `always_in_single_run`, ignorando os meses permitidos.
        """
        now = datetime.now()

        for task in self.tasks:
            if task.always_in_single_run or task.schedule.has_slot_at(now):
                self._execute(task)

    def start(self) -> None:
        """Inicia o ciclo contínuo de execução das tarefas"""
        run_immediately = self.config['SCHEDULE_RUN_IMMEDIATELY']

        for task in self.tasks:
            if run_immediately and task.schedule.is_within_window(datetime.now()):
                self._execute_scheduled(task)
            else:
                task.next_run = task.schedule.next_after(datetime.now())

        self._log_configuration()

        try:
            while True:
                self._run_due_tasks()
                time.sleep(self._seconds_until_next_run())
        except KeyboardInterrupt:
            self.logger.info('Recebido sinal de interrupção. Encerrando...')
        except Exception as e:
            self.logger.error(f'Erro inesperado no scheduler: {e}')
            raise
