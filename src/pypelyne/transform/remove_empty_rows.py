from pypelyne import Transform as Super

class Transform(Super):

    def process(self):
        """
        """
        line = self._dataset.first() # le uma linha
        while line:
            print(line)
            if not ''.join(line).strip():
                self._dataset.current_delete()
                line = self._dataset.current()
                continue
            line = self._dataset.next()